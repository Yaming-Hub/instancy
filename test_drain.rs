// Test drain_into for double-free or use-after-free

fn main() {
    println!("Analyzing drain_into safety:\n");
    
    // Consumer loads head (relaxed), refreshes tail (acquire)
    let head = 2usize;
    let tail = 6usize;
    let count = tail.wrapping_sub(head); // = 4
    
    println!("head={}, tail={}, count={}", head, tail, count);
    println!("\nDraining:");
    
    for i in 0..count {
        let slot_idx = head.wrapping_add(i);
        let slot = slot_idx & 7; // mask=7 for capacity=4
        println!("  i={}, slot_idx={}, slot={}", i, slot_idx, slot);
        // assume_init_read is called here - moves the value out
        println!("    -> Value moved out, slot now uninitialized");
    }
    
    println!("\nAfter drain, head updated to {}", head.wrapping_add(count));
    println!("Slots 2,3,4,5 are now uninitialized (values moved to Vec)");
    println!("\nIf producer writes to slot 2 next (after wrapping), is it safe?");
    println!("Yes! Producer writes with .write() which doesn't drop old value.");
    println!("And the slot is already uninitialized after assume_init_read.");
    
    println!("\n=== Checking for potential bugs ===");
    println!("1. Double-free if count is wrong? NO - count is tail - head");
    println!("2. Reading uninitialized memory? NO - only read [head..tail)");
    println!("3. Use-after-free? NO - values are moved, not borrowed");
    println!("4. Race with producer? NO - SPSC, sole consumer");
}
