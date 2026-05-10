//! # Panic Recovery with `catch_panics`
//!
//! Demonstrates how operator panics behave with and without panic recovery.
//!
//! Run with: `cargo run --example panic_recovery --all-features`

use std::any::Any;
use std::panic::{self, AssertUnwindSafe};

use instancy::DataflowBuilder;
use instancy::{RuntimeConfig, RuntimeHandle, SpawnOptions, SpawnedDataflow};

fn main() {
    let rt = RuntimeHandle::new(RuntimeConfig::default()).expect("runtime creation failed");

    println!("=== Run 1: With panic recovery ===");
    run_with_recovery(&rt);

    println!("\n=== Run 2: Without panic recovery ===");
    run_without_recovery(&rt);
}

fn spawn_divide_flow(rt: &RuntimeHandle, name: &str, catch_panics: bool) -> SpawnedDataflow<u64> {
    let builder = DataflowBuilder::<u64>::new(name.to_string());
    builder.catch_panics(catch_panics);

    let input = builder.input::<i32>("data").unwrap();
    input
        .map("divide", |_t, x| {
            if x == 0 {
                panic!("division by zero!");
            }
            100 / x
        })
        .output("results")
        .unwrap();

    let dataflow = builder.build().expect("build failed");
    rt.spawn(dataflow, SpawnOptions::default())
        .expect("spawn failed")
}

fn run_with_recovery(rt: &RuntimeHandle) {
    with_silent_panic_hook(|| {
        let mut handle = spawn_divide_flow(rt, "panic_demo_recovered", true);
        let sender = handle.take_input::<i32>("data").expect("input port");
        let output = handle.take_output::<i32>("results").expect("output port");

        println!("Sending: [10, 5, 0, 2]");
        sender.send(0, vec![10, 5, 0, 2]).expect("send failed");
        drop(sender);

        let results = output.collect_data();
        print_batches("Output collected before shutdown:", &results);

        match handle.join_blocking() {
            Ok(()) => println!("Completed successfully"),
            Err(e) => println!("Caught error: {e}"),
        }
    });
}

fn run_without_recovery(rt: &RuntimeHandle) {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        with_silent_panic_hook(|| {
            let mut handle = spawn_divide_flow(rt, "panic_demo_unrecovered", false);
            let sender = handle.take_input::<i32>("data").expect("input port");
            let output = handle.take_output::<i32>("results").expect("output port");

            println!("Sending: [10, 5, 0, 2]");
            sender.send(0, vec![10, 5, 0, 2]).expect("send failed");
            drop(sender);

            let results = output.collect_data();
            print_batches("Output collected before shutdown:", &results);

            handle.join_blocking()
        })
    }));

    match result {
        Ok(Ok(())) => println!("Completed successfully"),
        Ok(Err(e)) => println!("Runtime reported failure after the panic: {e}"),
        Err(payload) => println!("Panic escaped to the caller: {}", panic_message(payload)),
    }
}

fn print_batches(label: &str, batches: &[(u64, Vec<i32>)]) {
    println!("{label}");
    if batches.is_empty() {
        println!("  <no output>");
        return;
    }

    for (time, batch) in batches {
        println!("  t={time}: {batch:?}");
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_string(),
            Err(_) => "non-string panic payload".to_string(),
        },
    }
}

fn with_silent_panic_hook<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    let previous_hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));

    let result = panic::catch_unwind(AssertUnwindSafe(f));
    panic::set_hook(previous_hook);

    match result {
        Ok(value) => value,
        Err(payload) => panic::resume_unwind(payload),
    }
}
