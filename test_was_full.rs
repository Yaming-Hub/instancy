// Test was_full logic for wake optimization

fn main() {
    println!("=== Testing was_full wake optimization ===\n");
    
    let capacity = 4usize;
    
    // Scenario: Buffer is full
    let head = 0usize;
    let cached_tail = 4usize;
    
    let len_before = cached_tail.wrapping_sub(head);
    let was_full = len_before >= capacity;
    
    println!("Before pull: head={}, cached_tail={}, len={}", head, cached_tail, len_before);
    println!("was_full={} (should wake producer)", was_full);
    
    // Consumer pulls
    let new_head = head.wrapping_add(1);
    println!("\nAfter pull: head={}, cached_tail={}, len={}", 
             new_head, cached_tail, cached_tail.wrapping_sub(new_head));
    
    println!("\n=== Issue: Using stale cached_tail for was_full ===");
    println!("In pull(), we check: was_full = len_from(head, cached_tail) >= capacity");
    println!("But cached_tail might be stale!");
    
    println!("\nScenario:");
    println!("1. Consumer last refreshed cached_tail=4");
    println!("2. Producer pushed more, tail is now 5");
    println!("3. Consumer pulls: head goes 0->1");
    println!("4. was_full check uses stale cached_tail=4");
    
    let head2 = 0usize;
    let cached_tail_stale = 4usize;
    let real_tail = 5usize;
    
    let len_with_stale = cached_tail_stale.wrapping_sub(head2);
    let was_full_stale = len_with_stale >= capacity;
    
    let len_with_real = real_tail.wrapping_sub(head2);
    let was_full_real = len_with_real >= capacity;
    
    println!("\nWith stale cache: len={}, was_full={}", len_with_stale, was_full_stale);
    println!("With real tail:   len={}, was_full={}", len_with_real, was_full_real);
    
    println!("\nBoth show was_full=true, so wake happens either way. OK!");
    
    println!("\n=== Can stale cache cause MISSED wake? ===");
    println!("Consumer cached_tail=3 (stale), real tail=4 (full)");
    let ct_stale = 3usize;
    let rt_real = 4usize;
    let h = 0usize;
    
    println!("Stale len={}, was_full={}", ct_stale - h, (ct_stale - h) >= capacity);
    println!("Real len={}, was_full={}", rt_real - h, (rt_real - h) >= capacity);
    println!("Stale shows NOT full, so NO wake!");
    println!("But producer is actually blocked with backpressure...");
    println!("\nHowever:");
    println!("- Next pull() will refresh cached_tail and see it's full");
    println!("- OR producer will push again later and get through");
    println!("- This is a performance issue (delayed wake), not a correctness bug");
}
