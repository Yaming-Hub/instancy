//! Operator breadth and random programs, in-process, against timely.

use instancy_differential::workloads::{SMALL, breadth_scenario, program_scenario};
use instancy_differential::{
    Observe, SETTINGS, differential, instancy_backend, require_progress_check_panic, run_engine,
    timely_backend,
};

#[test]
fn operator_breadth_matches_timely() {
    require_progress_check_panic();
    for (i, (workers, capacity)) in SETTINGS.into_iter().enumerate() {
        let sc = breadth_scenario(i as u64 + 1, workers, capacity, 1 + i % 2, SMALL);
        differential(
            &sc,
            |sc| timely_backend::start_breadth(sc, Observe::Capture),
            |sc| instancy_backend::start_breadth(sc, Observe::Capture),
        );
    }
}

/// 48 generated programs. Programs have no independent model, so the test
/// also requires most of them to produce totals: a generator that filtered
/// everything away would otherwise compare empty outputs.
#[test]
fn random_programs_match_timely() {
    require_progress_check_panic();
    let mut productive = 0;
    for seed in 1..=48u64 {
        let (workers, capacity) = SETTINGS[(seed as usize - 1) % 8];
        let sc = program_scenario(seed, workers, capacity, SMALL);
        let reference = match run_engine(timely_backend::start_program(&sc, Observe::Capture), &sc)
        {
            Ok(s) => s,
            Err(e) => panic!("reference/fixture failure:\n{e}"),
        };
        let candidate =
            match run_engine(instancy_backend::start_program(&sc, Observe::Capture), &sc) {
                Ok(s) => s,
                Err(e) => panic!("instancy failure:\n{e}"),
            };
        if let Err(e) = instancy_differential::compare(&sc, &reference, &candidate) {
            panic!("mismatch:\n{e}");
        }
        let totals = sc.probe_index(0, "totals");
        if reference.last().unwrap().keys().any(|o| o.probe == totals) {
            productive += 1;
        }
    }
    assert!(
        productive >= 40,
        "only {productive} of 48 programs produced totals"
    );
}
