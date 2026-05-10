// Simulate a race where consumer observes uninitialized memory

use std::sync::atomic::{AtomicUsize, Ordering};

fn main() {
    // Simulating SPSC ring buffer state
    let head = AtomicUsize::new(0);
    let tail = AtomicUsize::new(0);
    
    println!("=== Testing Memory Ordering ===\n");
    
    // PRODUCER THREAD:
    println!("Producer writes to slot[0]...");
    // (*slots[0].value.get()).write(data);  // Write happens here
    
    println!("Producer stores tail with Release ordering");
    tail.store(1, Ordering::Release);
    
    // CONSUMER THREAD:
    println!("\nConsumer loads head with Relaxed: {}", head.load(Ordering::Relaxed));
    let cached_tail = tail.load(Ordering::Acquire);
    println!("Consumer loads tail with Acquire: {}", cached_tail);
    
    println!("\nThe Acquire on tail synchronizes-with Release on tail.");
    println!("This creates happens-before: write to slot[0] -> tail.store -> tail.load -> read from slot[0]");
    println!("So consumer is guaranteed to see the initialized value!");
    
    println!("\n=== Testing potential race ===");
    println!("What if consumer has cached_tail=1 and reads WITHOUT re-loading?");
    println!("- Consumer already loaded tail with Acquire earlier");
    println!("- The first Acquire established synchronization");
    println!("- Subsequent reads of slot[0] still see the writes that happened-before that tail.store");
    println!("- So cached_tail is safe as long as it came from an Acquire load!");
}
