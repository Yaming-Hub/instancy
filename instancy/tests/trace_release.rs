//! The progress flight recorder (`progress::trace`) is compiled only into
//! builds with `debug_assertions`, the `progress-check` feature or the
//! `test-utils` feature (acceptance: a release build without them contains
//! none of it).
//!
//! This test binary runs a dataflow, so it links every part of the runtime
//! that would reach the recorder, then looks for the recorder's
//! environment variable in its own executable: present exactly where the
//! recorder is compiled in. Run it with `cargo test --release --test
//! trace_release` (no features) to check a release build.

use instancy::{DataflowBuilder, RuntimeConfig, RuntimeHandle, SpawnOptions};

/// The recorder's variable, built at run time so that this test does not
/// put it in the executable itself.
fn needle() -> Vec<u8> {
    "RID_ECART_YCNATSNI"
        .chars()
        .rev()
        .collect::<String>()
        .into_bytes()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    let first = needle[0];
    haystack
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == first)
        .any(|(at, _)| haystack[at..].starts_with(needle))
}

#[test]
fn the_recorder_is_compiled_in_exactly_where_it_is_gated() {
    // Reach the code that would record: spawn and run a small dataflow.
    let rt = RuntimeHandle::new(RuntimeConfig::default()).unwrap();
    let mut multi = rt
        .spawn_multi(
            "trace-release",
            2,
            |b: &mut DataflowBuilder<u64>| {
                b.input::<u64>("in")?
                    .map("inc", |_t, x| x + 1)
                    .for_each("sink", |_t, _x| {});
                Ok(())
            },
            SpawnOptions::default(),
        )
        .unwrap();
    for worker in 0..2 {
        let input = multi.take_input::<u64>(worker, "in").unwrap();
        input.send(1, vec![1, 2, 3]).unwrap();
    }
    multi.join_blocking().unwrap();

    let exe = std::fs::read(std::env::current_exe().unwrap()).unwrap();
    let compiled_in = cfg!(any(
        debug_assertions,
        feature = "progress-check",
        feature = "test-utils"
    ));
    assert_eq!(
        contains(&exe, &needle()),
        compiled_in,
        "the recorder must be compiled in exactly with debug assertions, `progress-check` or \
         `test-utils`"
    );
}
