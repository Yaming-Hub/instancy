/// Demonstrates binary and concat operators for merging multiple streams.
use instancy::dataflow::DataflowBuilder;
use instancy::dataflow::Pipe;
use instancy::runtime::SimpleRuntime;

fn main() {
    // --- Binary: pair two streams element-wise ---
    let builder = DataflowBuilder::<u64>::new("binary_demo");
    let names = builder.source("names", vec![
        (0u64, vec!["Alice".to_string(), "Bob".to_string()]),
    ]);
    let scores = builder.source("scores", vec![
        (0u64, vec![95i32, 87]),
    ]);

    let paired = names.binary::<i32, String, _>(scores, "pair", |names_in, scores_in, out| {
        let mut ns = Vec::new();
        while let Some((_t, data)) = names_in.next() {
            ns.extend(data.iter().cloned());
        }
        let mut ss = Vec::new();
        while let Some((_t, data)) = scores_in.next() {
            ss.extend(data.iter().cloned());
        }
        if !ns.is_empty() {
            let pairs: Vec<String> = ns.iter().zip(ss.iter())
                .map(|(n, s)| format!("{n}: {s}"))
                .collect();
            out.push_vec(0, pairs);
        }
        Ok(())
    });

    let binary_out = paired.output("binary_results");

    let dataflow = builder.build().unwrap();
    SimpleRuntime::new().run(dataflow).unwrap();

    println!("=== Binary (pair names with scores) ===");
    for (t, batch) in binary_out.collector().lock().unwrap().iter() {
        println!("  t={t}: {batch:?}");
    }

    // --- Concat: merge three streams ---
    let builder = DataflowBuilder::<u64>::new("concat_demo");
    let critical = builder.source("critical", vec![(0u64, vec!["[CRIT] disk full".to_string()])]);
    let warnings = builder.source("warnings", vec![(0u64, vec!["[WARN] high cpu".to_string()])]);
    let info = builder.source("info", vec![(0u64, vec!["[INFO] started".to_string()])]);

    let all_logs = Pipe::concat(vec![critical, warnings, info]);
    let concat_out = all_logs.output("all_logs");

    let dataflow = builder.build().unwrap();
    SimpleRuntime::new().run(dataflow).unwrap();

    println!("\n=== Concat (merge 3 log streams) ===");
    for (t, batch) in concat_out.collector().lock().unwrap().iter() {
        println!("  t={t}: {batch:?}");
    }

    // --- Merge: convenience for 2 streams ---
    let builder = DataflowBuilder::<u64>::new("merge_demo");
    let evens = builder.source("evens", vec![(0u64, vec![2, 4, 6])]);
    let odds = builder.source("odds", vec![(0u64, vec![1, 3, 5])]);

    let all = evens.merge(odds)
        .map("sort_label", |_t, x| format!("num={x}"));
    let merge_out = all.output("sorted");

    let dataflow = builder.build().unwrap();
    SimpleRuntime::new().run(dataflow).unwrap();

    println!("\n=== Merge (evens + odds → labeled) ===");
    for (t, batch) in merge_out.collector().lock().unwrap().iter() {
        println!("  t={t}: {batch:?}");
    }
}
