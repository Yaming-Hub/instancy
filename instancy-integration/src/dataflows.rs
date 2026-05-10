//! Predefined dataflow builders for cross-process integration tests.
//!
//! Each function builds a specific dataflow pattern on a [`DataflowBuilder`],
//! returning the input port name(s) and output port name.

use std::collections::HashMap;

use instancy::dataflow::DataflowBuilder;
use instancy::error::Result;

use crate::protocol::DataflowType;

/// Get port names for a dataflow type without building it.
pub fn port_names(dataflow_type: &DataflowType) -> (Vec<String>, String) {
    match dataflow_type {
        DataflowType::PassThrough
        | DataflowType::ExchangeRoundTrip
        | DataflowType::MultiEpochExchange
        | DataflowType::IterativeFilter
        | DataflowType::StagedFanOutFanIn { .. }
        | DataflowType::FilterAggregate { .. }
        | DataflowType::BranchMerge
        | DataflowType::DelayedAggregation { .. }
        | DataflowType::IterativeExchange { .. } => (vec!["data".into()], "results".into()),
        DataflowType::DistributedWordCount => (vec!["sentences".into()], "results".into()),
        DataflowType::DistributedJoin => (vec!["left".into(), "right".into()], "results".into()),
    }
}

/// Build a dataflow of the given type on the provided builder.
///
/// Returns `(input_port_names, output_port_name)`.
pub fn build_dataflow(
    dataflow_type: &DataflowType,
    builder: &mut DataflowBuilder<u64>,
) -> Result<(Vec<String>, String)> {
    match dataflow_type {
        DataflowType::PassThrough => build_pass_through(builder),
        DataflowType::ExchangeRoundTrip => build_exchange_round_trip(builder),
        DataflowType::MultiEpochExchange => build_multi_epoch_exchange(builder),
        DataflowType::DistributedWordCount => build_distributed_word_count(builder),
        DataflowType::IterativeFilter => build_iterative_filter(builder),
        DataflowType::DistributedJoin => build_distributed_join(builder),
        DataflowType::StagedFanOutFanIn {
            fan_out_parallelism,
        } => build_staged_fan_out_fan_in(builder, *fan_out_parallelism),
        DataflowType::FilterAggregate { threshold } => build_filter_aggregate(builder, *threshold),
        DataflowType::BranchMerge => build_branch_merge(builder),
        DataflowType::DelayedAggregation { delay_offset } => {
            build_delayed_aggregation(builder, *delay_offset)
        }
        DataflowType::IterativeExchange { threshold } => {
            build_iterative_exchange(builder, *threshold)
        }
    }
}

/// PassThrough: source → map(identity with marker) → output.
/// No exchange — data stays on the node where it was fed.
fn build_pass_through(builder: &mut DataflowBuilder<u64>) -> Result<(Vec<String>, String)> {
    let input = builder.input::<Vec<u8>>("data");
    input.map("identity", |_t, x| x).output("results");
    Ok((vec!["data".into()], "results".into()))
}

/// ExchangeRoundTrip: source → exchange_by_hash(key) → map → output.
/// Data is repartitioned across workers/nodes by the first 8 bytes as u64 hash.
fn build_exchange_round_trip(builder: &mut DataflowBuilder<u64>) -> Result<(Vec<String>, String)> {
    let input = builder.input::<(u64, String)>("data");
    input
        .exchange_by_hash("partition", |item: &(u64, String)| item.0)
        .map("tag", |_t, item| item)
        .output("results");
    Ok((vec!["data".into()], "results".into()))
}

/// MultiEpochExchange: source → exchange(key) → unary_notify(sum per epoch) → output.
/// Tests frontier propagation across many epochs.
fn build_multi_epoch_exchange(builder: &mut DataflowBuilder<u64>) -> Result<(Vec<String>, String)> {
    let input = builder.input::<(u64, i64)>("data");
    input
        .exchange_by_hash("partition", |item: &(u64, i64)| item.0)
        .unary_notify("epoch_sum", {
            let mut pending: HashMap<u64, Vec<(u64, i64)>> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    pending.entry(time).or_default().extend(data);
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    if let Some(items) = pending.remove(&time) {
                        // Group by key and sum values
                        let mut sums: HashMap<u64, i64> = HashMap::new();
                        for (key, val) in items {
                            *sums.entry(key).or_default() += val;
                        }
                        let results: Vec<(u64, i64)> = sums.into_iter().collect();
                        output.push_vec(time, results);
                    }
                }
                Ok(())
            }
        })
        .output("results");
    Ok((vec!["data".into()], "results".into()))
}

/// DistributedWordCount: source → flat_map(split) → exchange(word) → unary_notify(count) → output.
fn build_distributed_word_count(
    builder: &mut DataflowBuilder<u64>,
) -> Result<(Vec<String>, String)> {
    let input = builder.input::<String>("sentences");
    input
        .flat_map("split", |_t, sentence| {
            sentence
                .split_whitespace()
                .map(|w| w.to_lowercase())
                .collect::<Vec<_>>()
        })
        .exchange("by_word", |word: &String| word.clone())
        .unary_notify("count", {
            let mut pending: HashMap<u64, HashMap<String, u64>> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, words)) = input.next() {
                    let counts = pending.entry(time).or_default();
                    for word in words {
                        *counts.entry(word).or_default() += 1;
                    }
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    if let Some(counts) = pending.remove(&time) {
                        let results: Vec<(String, u64)> = counts.into_iter().collect();
                        output.push_vec(time, results);
                    }
                }
                Ok(())
            }
        })
        .output("results");
    Ok((vec!["sentences".into()], "results".into()))
}

/// IterativeFilter: source → iterate(decay + filter via exchange) → output.
/// Each iteration decrements values and filters out those that reach zero.
fn build_iterative_filter(builder: &mut DataflowBuilder<u64>) -> Result<(Vec<String>, String)> {
    use instancy::dataflow::dataflow_builder::IterateResult;

    let input = builder.input::<(u64, i64)>("data");
    input
        .iterate::<u32>("converge", 10u32, |iter_stream| {
            let processed = iter_stream
                .exchange_by_hash("shuffle", |item: &(u64, i64)| item.0)
                .map("decay", |_t, (key, val)| (key, val - 1));
            let done = processed.clone().filter("done", |_t, item| item.1 <= 1);
            let again = processed.filter("again", |_t, item| item.1 > 1);
            IterateResult {
                feedback: again,
                output: done,
            }
        })
        .output("results");
    Ok((vec!["data".into()], "results".into()))
}

/// DistributedJoin: two inputs → exchange both → binary join on key → output.
fn build_distributed_join(builder: &mut DataflowBuilder<u64>) -> Result<(Vec<String>, String)> {
    let left_input = builder.input::<(u64, String)>("left");
    let right_input = builder.input::<(u64, i64)>("right");

    let left = left_input.exchange_by_hash("left_partition", |item: &(u64, String)| item.0);
    let right = right_input.exchange_by_hash("right_partition", |item: &(u64, i64)| item.0);

    left.binary(right, "join", {
        let mut left_buf: HashMap<u64, Vec<(u64, String)>> = HashMap::new();
        let mut right_buf: HashMap<u64, Vec<(u64, i64)>> = HashMap::new();

        move |left_in, right_in, output| {
            while let Some((time, data)) = left_in.next() {
                left_buf.entry(time).or_default().extend(data);
            }
            while let Some((time, data)) = right_in.next() {
                right_buf.entry(time).or_default().extend(data);
            }
            // Emit matches for timestamps present in both sides, then drain
            // processed entries to avoid duplicate emission on subsequent calls.
            let common_times: Vec<u64> = left_buf
                .keys()
                .filter(|t| right_buf.contains_key(t))
                .copied()
                .collect();
            for t in common_times {
                let lefts = left_buf.remove(&t).unwrap();
                let rights = right_buf.remove(&t).unwrap();
                let mut joined = Vec::new();
                for (lk, lv) in &lefts {
                    for (rk, rv) in &rights {
                        if lk == rk {
                            joined.push((lk.clone(), lv.clone(), *rv));
                        }
                    }
                }
                if !joined.is_empty() {
                    output.push_vec(t, joined);
                }
            }
            Ok(())
        }
    })
    .output("results");

    Ok((vec!["left".into(), "right".into()], "results".into()))
}

/// StagedFanOutFanIn: source → map → exchange_to(N) → map → gather → output.
fn build_staged_fan_out_fan_in(
    builder: &mut DataflowBuilder<u64>,
    fan_out_parallelism: usize,
) -> Result<(Vec<String>, String)> {
    if fan_out_parallelism == 0 {
        return Err(instancy::error::Error::Custom(
            "fan_out_parallelism must be > 0".into(),
        ));
    }

    let input = builder.input::<i64>("data");
    input
        .map("stage_one", |_t, x| x * 2)
        .exchange_by_hash_to("fan_out", fan_out_parallelism, |x: &i64| *x as u64)
        .map("stage_two", |_t, x| x + 1)
        .gather("fan_in")
        .output("results");
    Ok((vec!["data".into()], "results".into()))
}

/// FilterAggregate: source → filter(>threshold) → exchange → reduce(sum) → output.
fn build_filter_aggregate(
    builder: &mut DataflowBuilder<u64>,
    threshold: i64,
) -> Result<(Vec<String>, String)> {
    let input = builder.input::<i64>("data");
    input
        .filter("gt_threshold", move |_t, x| *x > threshold)
        .exchange_by_hash("aggregate_route", |_x| 0u64)
        .reduce("sum", |a, b| a + b)
        .output("results");
    Ok((vec!["data".into()], "results".into()))
}

/// BranchMerge: source → branch(even/odd) → map(path-specific) → merge → exchange → reduce → output.
fn build_branch_merge(builder: &mut DataflowBuilder<u64>) -> Result<(Vec<String>, String)> {
    let input = builder.input::<i64>("data");
    let (evens, odds) = input.branch("split", |_t, x| x % 2 == 0);
    evens
        .map("double_even", |_t, x| x * 2)
        .merge(odds.map("triple_odd", |_t, x| x * 3))
        .exchange_by_hash("aggregate_route", |_x| 0u64)
        .reduce("sum", |a, b| a + b)
        .output("results");
    Ok((vec!["data".into()], "results".into()))
}

/// DelayedAggregation: source → delay_batch(+offset) → exchange → unary_notify(sum per epoch) → output.
fn build_delayed_aggregation(
    builder: &mut DataflowBuilder<u64>,
    delay_offset: u64,
) -> Result<(Vec<String>, String)> {
    let input = builder.input::<i64>("data");
    input
        .delay_batch("delay", move |t| t + delay_offset)
        .exchange_by_hash("aggregate_route", |_x| 0u64)
        .unary_notify("epoch_sum", {
            let mut pending: HashMap<u64, Vec<i64>> = HashMap::new();
            move |input, output, ctx| {
                while let Some((time, data)) = input.next() {
                    pending.entry(time).or_default().extend(data);
                    ctx.notify_at(time);
                }
                while let Some(time) = ctx.next_notification() {
                    if let Some(items) = pending.remove(&time) {
                        let sum: i64 = items.into_iter().sum();
                        output.push_vec(time, vec![sum]);
                    }
                }
                Ok(())
            }
        })
        .output("results");
    Ok((vec!["data".into()], "results".into()))
}

/// IterativeExchange: source → iterate(exchange → map(double) → filter(<threshold) → feedback) → output.
fn build_iterative_exchange(
    builder: &mut DataflowBuilder<u64>,
    threshold: i64,
) -> Result<(Vec<String>, String)> {
    use instancy::dataflow::dataflow_builder::IterateResult;

    let input = builder.input::<i64>("data");
    input
        .iterate::<u32>("loop", 1u32, move |iter_stream| {
            let processed = iter_stream
                .exchange_by_hash("shuffle", |x: &i64| *x as u64)
                .map("double", |_t, x| x * 2);
            let feedback = processed
                .clone()
                .filter("again", move |_t, x| *x < threshold);
            let output = processed.filter("done", move |_t, x| *x >= threshold);
            IterateResult { feedback, output }
        })
        .output("results");
    Ok((vec!["data".into()], "results".into()))
}
