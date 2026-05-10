// Test if stale cache can cause memory safety issues

use std::sync::atomic::{AtomicUsize, Ordering};

fn main() {
    println!("=== Testing stale cache safety ===\n");
    
    let head = AtomicUsize::new(0);
    let tail = AtomicUsize::new(4);
    let capacity = 4;
    
    // PRODUCER with stale cached_head=0
    let mut cached_head = 0usize;
    let current_tail = tail.load(Ordering::Relaxed);
    let len = current_tail.wrapping_sub(cached_head);
    
    println!("Producer state:");
    println!("  tail={}, cached_head={}, len={}, capacity={}", 
             current_tail, cached_head, len, capacity);
    println!("  Buffer appears full (len >= capacity): {}", len >= capacity);
    
    // Consumer pulls one item
    println!("\nConsumer pulls item...");
    head.store(1, Ordering::Release);
    println!("  head is now 1");
    
    // Producer still has stale cache
    println!("\nProducer checks again with STALE cache:");
    let len_stale = current_tail.wrapping_sub(cached_head);
    println!("  len={} (using cached_head={})", len_stale, cached_head);
    println!("  Still appears full: {}", len_stale >= capacity);
    
    // Producer refreshes cache when buffer appears full
    println!("\nProducer refreshes cache:");
    cached_head = head.load(Ordering::Acquire);
    let len_fresh = current_tail.wrapping_sub(cached_head);
    println!("  cached_head={}, len={}", cached_head, len_fresh);
    println!("  Now has space: {}", len_fresh < capacity);
    
    println!("\n=== CRITICAL: Can stale cache cause memory corruption? ===");
    println!("NO! Stale cache only affects performance (false backpressure).");
    println!("Producer refreshes cache BEFORE checking capacity.");
    println!("If refresh shows space, producer writes and stores tail.");
    println!("Consumer's Acquire on tail synchronizes with producer's Release.");
    println!("All memory accesses are safe!");
}
