// Test is_exhausted TOCTOU

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

fn main() {
    let closed = Arc::new(AtomicBool::new(false));
    let head = Arc::new(AtomicUsize::new(5));
    let tail = Arc::new(AtomicUsize::new(5));
    
    let closed_clone = Arc::clone(&closed);
    let head_clone = Arc::clone(&head);
    let tail_clone = Arc::clone(&tail);
    
    // Consumer thread checks is_exhausted
    let consumer = thread::spawn(move || {
        // Step 1: Load closed
        let is_closed = closed_clone.load(Ordering::Acquire);
        println!("Consumer: closed={}", is_closed);
        
        // Simulate race: producer pushes item here
        println!("Consumer: <race window>");
        thread::sleep(std::time::Duration::from_millis(10));
        
        // Step 2: Load head and tail
        let h = head_clone.load(Ordering::Relaxed);
        let t = tail_clone.load(Ordering::Acquire);
        println!("Consumer: head={}, tail={}", h, t);
        
        let exhausted = is_closed && (h == t);
        println!("Consumer: is_exhausted={}", exhausted);
        exhausted
    });
    
    // Producer thread pushes item
    thread::sleep(std::time::Duration::from_millis(5));
    println!("Producer: pushing item...");
    tail.store(6, Ordering::Release);
    
    println!("Producer: closing channel...");
    closed.store(true, Ordering::Release);
    
    let result = consumer.join().unwrap();
    println!("\nResult: is_exhausted={}", result);
    println!("But there's 1 item in buffer! (head=5, tail=6)");
    println!("This is a TOCTOU bug!");
}
