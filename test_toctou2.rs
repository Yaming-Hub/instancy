// Real TOCTOU test for is_exhausted

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

fn main() {
    let closed = Arc::new(AtomicBool::new(true)); // Already closed
    let head = Arc::new(AtomicUsize::new(5));
    let tail = Arc::new(AtomicUsize::new(5)); // Empty
    
    let closed_clone = Arc::clone(&closed);
    let head_clone = Arc::clone(&head);
    let tail_clone = Arc::clone(&tail);
    
    // Consumer thread checks is_exhausted
    let consumer = thread::spawn(move || {
        // Step 1: Load closed - sees true
        let is_closed = closed_clone.load(Ordering::Acquire);
        println!("Consumer: closed={}", is_closed);
        
        if !is_closed {
            return false;
        }
        
        // Step 2: Load head
        let h = head_clone.load(Ordering::Relaxed);
        println!("Consumer: head={}", h);
        
        // RACE WINDOW: Another producer (impossible in SPSC!) pushes here
        println!("Consumer: <race window>");
        thread::sleep(std::time::Duration::from_millis(10));
        
        // Step 3: Load tail
        let t = tail_clone.load(Ordering::Acquire);
        println!("Consumer: tail={}", t);
        
        h == t
    });
    
    // Simulated producer (this violates SPSC but tests the TOCTOU)
    thread::sleep(std::time::Duration::from_millis(5));
    println!("Producer: pushing item to closed channel...");
    tail.store(6, Ordering::Release);
    
    let result = consumer.join().unwrap();
    println!("\nResult: is_exhausted={}", result);
    println!("Actual state: head=5, tail=6 (1 item)");
    
    if result {
        println!("BUG: Reported exhausted but buffer has items!");
    } else {
        println!("OK: Correctly reported not exhausted");
    }
    
    println!("\nBut wait - this scenario requires TWO PRODUCERS pushing to a closed channel!");
    println!("In real SPSC, once closed.store(true) happens, no more pushes can succeed.");
    println!("So this TOCTOU is not exploitable in correct SPSC usage.");
}
