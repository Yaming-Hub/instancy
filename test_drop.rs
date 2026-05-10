// Test if the Drop logic correctly handles all items

fn main() {
    println!("Testing Drop logic for ring buffer\n");
    
    // Scenario: capacity=4, so 5 slots (0-4 via mask=4)
    // But wait, capacity+1 = 5, next_power_of_two(5) = 8!
    let capacity = 4usize;
    let size = (capacity + 1).next_power_of_two();
    let mask = size - 1;
    
    println!("capacity={}, size={}, mask={}", capacity, size, mask);
    println!("Slots allocated: 0..{}", size);
    
    // Items at indices: head=2, tail=6
    let head = 2usize;
    let tail = 6usize;
    let count = tail.wrapping_sub(head);
    println!("\nhead={}, tail={}, count={}", head, tail, count);
    
    println!("\nItems to drop:");
    let mut idx = head;
    while idx != tail {
        let slot = idx & mask;
        println!("  idx={}, slot={}", idx, slot);
        idx = idx.wrapping_add(1);
    }
    
    println!("\n=== Testing wraparound drop ===");
    // After many operations: head=5, tail=usize::MAX-1
    // This is impossible (tail behind head), but let's see the loop behavior
    let head2 = 5usize;
    let tail2 = 3usize; // tail behind head after wrapping
    println!("head={}, tail={} (tail behind head)", head2, tail2);
    
    let mut idx2 = head2;
    let mut iterations = 0;
    while idx2 != tail2 && iterations < 10 {
        let slot = idx2 & mask;
        println!("  idx={}, slot={}", idx2, slot);
        idx2 = idx2.wrapping_add(1);
        iterations += 1;
    }
    
    if iterations >= 10 {
        println!("  ... (would continue for {} items total!)", tail2.wrapping_sub(head2));
    }
}
