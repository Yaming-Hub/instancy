//! Exchanged two-input totals and Product-time notifications, compared
//! against timely-dataflow at every prescribed cut.

use instancy_differential::{
    Observe, SETTINGS, differential, instancy_backend, product_scenario,
    require_progress_check_panic, timely_backend, totals_scenario,
};

#[test]
fn two_input_totals_release_only_when_both_inputs_pass_the_time() {
    require_progress_check_panic();
    for (i, (workers, capacity)) in SETTINGS.into_iter().enumerate() {
        let sc = totals_scenario(i as u64 + 1, workers, capacity);
        differential(
            &sc,
            |sc| timely_backend::start_totals(sc, Observe::Capture),
            |sc| instancy_backend::start_totals(sc, Observe::Capture),
        );
    }
}

#[test]
fn product_notifications_follow_the_incomparable_cut() {
    require_progress_check_panic();
    for (i, (workers, capacity)) in SETTINGS.into_iter().enumerate() {
        let sc = product_scenario(i as u64 + 1, workers, capacity);
        differential(
            &sc,
            |sc| timely_backend::start_product(sc, Observe::Capture),
            |sc| instancy_backend::start_product(sc, Observe::Capture),
        );
    }
}
