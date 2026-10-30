//! A time-bounded differential soak: generated scenarios of every streaming
//! kind, settings and instancy configurations until `DIFF_SOAK_SECS`
//! (default 600) elapse. Ignored by default; run with
//! `cargo nextest run --profile soak --run-ignored only soak`.

use std::time::{Duration, Instant};

use instancy_differential::workloads::{
    StreamShape, breadth_scenario, program_scenario, scaled_nested_scenario,
};
use instancy_differential::{
    InstancyConfig, Observe, Rng, SETTINGS, differential, instancy_backend,
    require_progress_check_panic, timely_backend,
};

#[test]
#[ignore = "long-running; run explicitly with the soak profile"]
fn soak_differential() {
    require_progress_check_panic();
    let secs: u64 = std::env::var("DIFF_SOAK_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(600);
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut runs = [0usize; 3];
    let mut seed = 10_000u64;
    while Instant::now() < deadline {
        seed += 1;
        let mut rng = Rng::new(seed);
        let (workers, capacity) = SETTINGS[rng.below(8) as usize];
        let config = InstancyConfig {
            pool_threads: 1 + rng.below(4) as usize,
            tokio_threads: 1 + rng.below(2) as usize,
            per_stage: rng.below(2) == 0,
            io_async: rng.below(2) == 0,
        };
        let shape = StreamShape {
            epochs: 4 + rng.below(40),
            max_per_epoch: 1 + rng.below(30),
            extend_every: 1 + rng.below(6),
        };
        match seed % 3 {
            0 => {
                let sc = program_scenario(seed, workers, capacity, shape).with_instancy(config);
                differential(
                    &sc,
                    |sc| timely_backend::start_program(sc, Observe::Capture),
                    |sc| instancy_backend::start_program(sc, Observe::Capture),
                );
            }
            1 => {
                let graphs = 1 + rng.below(3) as usize;
                let sc =
                    breadth_scenario(seed, workers, capacity, graphs, shape).with_instancy(config);
                differential(
                    &sc,
                    |sc| timely_backend::start_breadth(sc, Observe::Capture),
                    |sc| instancy_backend::start_breadth(sc, Observe::Capture),
                );
            }
            _ => {
                let graphs = 1 + rng.below(8) as usize;
                let outer = 1 + rng.below(3);
                let sc = scaled_nested_scenario(seed, workers, capacity, graphs, shape, outer)
                    .with_instancy(config);
                differential(
                    &sc,
                    |sc| timely_backend::start_nested(sc, Observe::Capture),
                    |sc| instancy_backend::start_nested(sc, Observe::Capture),
                );
            }
        }
        runs[(seed % 3) as usize] += 1;
    }
    println!(
        "soak: {} scenarios in {secs}s (programs {}, breadth {}, nested {}), last seed {seed}",
        runs.iter().sum::<usize>(),
        runs[0],
        runs[1],
        runs[2]
    );
    assert!(
        runs.iter().all(|n| *n > 0),
        "every scenario kind ran: {runs:?}"
    );
}
