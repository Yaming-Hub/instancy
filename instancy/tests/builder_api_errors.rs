//! Regression tests for builder API calls that used to panic inside the library.

use instancy::DataflowBuilder;

/// `Pipe::collect()` auto-names its output `__collect_N`. A user output with
/// that name used to make the library panic on an internal `expect`.
#[test]
fn collect_skips_output_names_already_taken() {
    let r = std::panic::catch_unwind(|| {
        let builder = DataflowBuilder::<u64>::new("collect-collision");
        let src = builder.source("s", vec![(0u64, vec![1i32, 2, 3])]);
        src.clone().output("__collect_0").unwrap();
        src.clone().output("__collect_1").unwrap();
        let _c = src.collect();
        let df = builder.build().unwrap();
        let mut names = df.output_names();
        names.sort();
        names.iter().map(|s| s.to_string()).collect::<Vec<_>>()
    });
    let names = r.expect("Pipe::collect() panicked on an output-name collision");
    assert_eq!(names.len(), 3, "collect output missing: {names:?}");
}
