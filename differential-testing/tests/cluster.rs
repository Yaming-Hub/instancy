//! The four recipes with instancy in cluster mode (two loopback-TCP nodes,
//! alternating dedicated and shared transport) against the in-process timely
//! reference with the same total worker count.

#![cfg(feature = "cluster")]

use instancy_differential::cluster_backend::{self, CLUSTER_SETTINGS, transport_for};
use instancy_differential::{
    Observe, differential, nested_scenario, product_scenario, require_progress_check_panic,
    timely_backend, totals_scenario,
};

#[test]
fn cluster_two_input_totals_match_timely() {
    require_progress_check_panic();
    for (i, (workers, capacity)) in CLUSTER_SETTINGS.into_iter().enumerate() {
        let seed = i as u64 + 1;
        let sc = totals_scenario(seed, workers, capacity);
        differential(
            &sc,
            |sc| timely_backend::start_totals(sc, Observe::Capture),
            |sc| cluster_backend::start_totals(sc, Observe::Capture, transport_for(seed)),
        );
    }
}

#[test]
fn cluster_product_notifications_match_timely() {
    require_progress_check_panic();
    for (i, (workers, capacity)) in CLUSTER_SETTINGS.into_iter().enumerate() {
        let seed = i as u64 + 1;
        let sc = product_scenario(seed, workers, capacity);
        differential(
            &sc,
            |sc| timely_backend::start_product(sc, Observe::Capture),
            |sc| cluster_backend::start_product(sc, Observe::Capture, transport_for(seed)),
        );
    }
}

#[test]
fn cluster_nested_feedback_notifications_match_timely() {
    require_progress_check_panic();
    for (i, (workers, capacity)) in CLUSTER_SETTINGS.into_iter().enumerate() {
        let seed = i as u64 + 1;
        let sc = nested_scenario(seed, workers, capacity, 1);
        differential(
            &sc,
            |sc| timely_backend::start_nested(sc, Observe::Capture),
            |sc| cluster_backend::start_nested(sc, Observe::Capture, transport_for(seed)),
        );
    }
}

#[test]
fn cluster_live_dataflows_progress_independently() {
    require_progress_check_panic();
    for (i, (workers, capacity)) in CLUSTER_SETTINGS.into_iter().enumerate() {
        let seed = i as u64 + 1;
        let graphs = if i % 2 == 0 { 2 } else { 8 };
        let sc = nested_scenario(seed, workers, capacity, graphs);
        differential(
            &sc,
            |sc| timely_backend::start_nested(sc, Observe::Capture),
            |sc| cluster_backend::start_nested(sc, Observe::Capture, transport_for(seed)),
        );
    }
}

#[test]
fn cluster_operator_breadth_matches_timely() {
    use instancy_differential::workloads::{SMALL, breadth_scenario};
    require_progress_check_panic();
    for (i, (workers, capacity)) in CLUSTER_SETTINGS.into_iter().enumerate() {
        let seed = i as u64 + 1;
        let sc = breadth_scenario(seed, workers, capacity, 1 + i % 2, SMALL);
        differential(
            &sc,
            |sc| timely_backend::start_breadth(sc, Observe::Capture),
            |sc| cluster_backend::start_breadth(sc, Observe::Capture, transport_for(seed)),
        );
    }
}

#[test]
fn cluster_random_programs_match_timely() {
    use instancy_differential::workloads::{SMALL, program_scenario};
    require_progress_check_panic();
    for seed in 1..=16u64 {
        let (workers, capacity) = CLUSTER_SETTINGS[(seed as usize - 1) % 8];
        let sc = program_scenario(seed, workers, capacity, SMALL);
        differential(
            &sc,
            |sc| timely_backend::start_program(sc, Observe::Capture),
            |sc| cluster_backend::start_program(sc, Observe::Capture, transport_for(seed)),
        );
    }
}
