//! The breadth, nested and random-program scenarios under different
//! instancy execution configurations: pool size, per-stage parallelism and
//! asynchronous I/O. Timely is unchanged.

use instancy_differential::workloads::{
    SMALL, breadth_scenario, program_scenario, scaled_nested_scenario,
};
use instancy_differential::{
    InstancyConfig, Observe, SETTINGS, differential, instancy_backend,
    require_progress_check_panic, timely_backend,
};

fn sweep(config: InstancyConfig) {
    require_progress_check_panic();
    for (i, (workers, capacity)) in SETTINGS.into_iter().enumerate() {
        let seed = 400 + i as u64;
        let sc = breadth_scenario(seed, workers, capacity, 1, SMALL).with_instancy(config);
        differential(
            &sc,
            |sc| timely_backend::start_breadth(sc, Observe::Capture),
            |sc| instancy_backend::start_breadth(sc, Observe::Capture),
        );
        let sc = scaled_nested_scenario(seed, workers, capacity, 2, SMALL, 2).with_instancy(config);
        differential(
            &sc,
            |sc| timely_backend::start_nested(sc, Observe::Capture),
            |sc| instancy_backend::start_nested(sc, Observe::Capture),
        );
        let sc = program_scenario(seed, workers, capacity, SMALL).with_instancy(config);
        differential(
            &sc,
            |sc| timely_backend::start_program(sc, Observe::Capture),
            |sc| instancy_backend::start_program(sc, Observe::Capture),
        );
    }
}

fn base() -> InstancyConfig {
    InstancyConfig::default()
}

#[test]
fn one_pool_thread() {
    sweep(InstancyConfig {
        pool_threads: 1,
        tokio_threads: 1,
        ..base()
    });
}

#[test]
fn four_pool_threads() {
    sweep(InstancyConfig {
        pool_threads: 4,
        tokio_threads: 4,
        ..base()
    });
}

#[test]
fn per_stage_parallelism() {
    sweep(InstancyConfig {
        per_stage: true,
        ..base()
    });
}

#[test]
fn async_io() {
    sweep(InstancyConfig {
        io_async: true,
        ..base()
    });
}

#[test]
fn combined_configuration() {
    sweep(InstancyConfig {
        pool_threads: 3,
        tokio_threads: 1,
        per_stage: true,
        io_async: true,
    });
}
