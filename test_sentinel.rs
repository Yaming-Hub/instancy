fn main() {
    println!("=== Testing capacity + 1 sentinel slot ===\n");
    
    // User requests capacity=4
    let capacity = 4usize;
    let size = (capacity + 1).next_power_of_two();
    let mask = size - 1;
    
    println!("capacity={} (user-requested)", capacity);
    println!("size={} (allocated slots)", size);
    println!("mask={} (for wrapping)", mask);
    
    println!("\nFilling buffer to capacity:");
    let head = 0usize;
    let mut tail = 0usize;
    
    for i in 0..capacity {
        let slot = tail & mask;
        println!("  push {}: tail={}, slot={}", i, tail, slot);
        tail += 1;
    }
    
    let len = tail.wrapping_sub(head);
    println!("\nAfter {} pushes: head={}, tail={}, len={}", capacity, head, tail, len);
    println!("Is full? len >= capacity: {}", len >= capacity);
    
    // Try to push one more
    println!("\nTrying to push one more:");
    let would_be_slot = tail & mask;
    println!("  Would write to slot={}", would_be_slot);
    println!("  But len={} >= capacity={}, so BLOCKED", len, capacity);
    
    println!("\n=== The sentinel slot ===");
    println!("With capacity=4, slots are [0,1,2,3,4,5,6,7] (size=8)");
    println!("When head=0 and tail=4, buffer is FULL (4 items)");
    println!("Slot 4 is never written to - it's the 'sentinel' that distinguishes");
    println!("full (tail=4, head=0) from empty (tail=0, head=0)");
    
    println!("\n=== What if capacity+1 is not a power of 2? ===");
    let cap2 = 5usize;
    let size2 = (cap2 + 1).next_power_of_two();
    println!("capacity={}, capacity+1={}, next_power_of_two={}", cap2, cap2+1, size2);
    println!("This over-allocates: user wants 5 items, gets 8 slots");
    println!("Wastes memory but ensures mask works correctly");
}
