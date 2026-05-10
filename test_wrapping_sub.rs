fn main() {
    // The wrapping_sub(head, tail) pattern
    let tail: usize = 0;
    let head: usize = 1; // Consumer has advanced
    
    // This is wrong! len should be 0 (empty), but wrapping_sub gives MAX
    let len = tail.wrapping_sub(head);
    println!("tail={}, head={}, len={}", tail, head, len);
    println!("Is this correct? tail < head means buffer is EMPTY, but len={}", len);
    
    // For a ring buffer, the length formula tail.wrapping_sub(head) assumes:
    // - tail >= head in the logical sense
    // - When consumer advances head past tail, we have an empty buffer
    
    // But wrapping_sub doesn't know about the ring semantics
    println!("\nTesting various states:");
    
    // Empty buffer
    println!("Empty: tail=5, head=5, len={}", (5usize).wrapping_sub(5));
    
    // 3 items
    println!("3 items: tail=8, head=5, len={}", (8usize).wrapping_sub(5));
    
    // Consumer ahead of producer (impossible in correct SPSC, but shows the issue)
    println!("Consumer ahead: tail=5, head=8, len={}", (5usize).wrapping_sub(8));
}
