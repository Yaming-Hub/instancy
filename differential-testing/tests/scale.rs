//! The same recipes at larger scale: many epochs, thousands of records and
//! many concurrent dataflows. Each test stays within the 120-second
//! per-process bound.

use instancy_differential::workloads::{StreamShape, breadth_scenario, scaled_nested_scenario};
use instancy_differential::{
    Observe, differential, instancy_backend, require_progress_check_panic, timely_backend,
};

const LONG: StreamShape = StreamShape {
    epochs: 120,
    max_per_epoch: 40,
    extend_every: 10,
};

/// Nested loops cost instancy about 0.4 s of wall clock per checkpoint here
/// (diagnosed with `DIFF_TIMING=1`: 145 checkpoints took 60 s against
/// timely's 15-23 s), so each scaled nested scenario is its own test with
/// 80 epochs to stay inside the 120-second process bound.
const NESTED_LONG: StreamShape = StreamShape {
    epochs: 80,
    max_per_epoch: 40,
    extend_every: 10,
};

#[test]
fn scaled_nested_stream_three_workers_matches_timely() {
    require_progress_check_panic();
    let sc = scaled_nested_scenario(101, 3, 2, 2, NESTED_LONG, 3);
    differential(
        &sc,
        |sc| timely_backend::start_nested(sc, Observe::Capture),
        |sc| instancy_backend::start_nested(sc, Observe::Capture),
    );
}

#[test]
fn scaled_nested_stream_capacity_one_matches_timely() {
    require_progress_check_panic();
    let sc = scaled_nested_scenario(102, 2, 1, 3, NESTED_LONG, 3);
    differential(
        &sc,
        |sc| timely_backend::start_nested(sc, Observe::Capture),
        |sc| instancy_backend::start_nested(sc, Observe::Capture),
    );
}

#[test]
fn scaled_breadth_streams_match_timely() {
    require_progress_check_panic();
    for (seed, workers, capacity) in [(201, 3, 1), (202, 2, 1024)] {
        let sc = breadth_scenario(seed, workers, capacity, 1, LONG);
        differential(
            &sc,
            |sc| timely_backend::start_breadth(sc, Observe::Capture),
            |sc| instancy_backend::start_breadth(sc, Observe::Capture),
        );
    }
}

#[test]
fn many_live_dataflows_match_timely() {
    require_progress_check_panic();
    let shape = StreamShape {
        epochs: 12,
        max_per_epoch: 16,
        extend_every: 4,
    };
    let sc = scaled_nested_scenario(301, 2, 2, 24, shape, 2);
    differential(
        &sc,
        |sc| timely_backend::start_nested(sc, Observe::Capture),
        |sc| instancy_backend::start_nested(sc, Observe::Capture),
    );
}
