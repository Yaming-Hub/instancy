//! Nested feedback with notification, alone and as several live dataflows
//! sharing one worker set, compared against timely-dataflow at every
//! prescribed cut.

use instancy_differential::{
    Observe, SETTINGS, differential, instancy_backend, nested_scenario,
    require_progress_check_panic, timely_backend,
};

#[test]
fn nested_feedback_notifications_hold_until_the_root_epoch_passes() {
    require_progress_check_panic();
    for (i, (workers, capacity)) in SETTINGS.into_iter().enumerate() {
        let sc = nested_scenario(i as u64 + 1, workers, capacity, 1);
        differential(
            &sc,
            |sc| timely_backend::start_nested(sc, Observe::Capture),
            |sc| instancy_backend::start_nested(sc, Observe::Capture),
        );
    }
}

#[test]
fn live_dataflows_on_one_worker_set_progress_independently() {
    require_progress_check_panic();
    for (i, (workers, capacity)) in SETTINGS.into_iter().enumerate() {
        let graphs = if i % 2 == 0 { 2 } else { 8 };
        let sc = nested_scenario(i as u64 + 1, workers, capacity, graphs);
        differential(
            &sc,
            |sc| timely_backend::start_nested(sc, Observe::Capture),
            |sc| instancy_backend::start_nested(sc, Observe::Capture),
        );
    }
}
