// Full scenario: producer blocking and consumer waking

fn main() {
    println!("=== Full scenario: Producer backpressure and wake ===\n");
    
    let capacity = 4usize;
    let mut producer_cached_head = 0usize;
    let mut consumer_cached_tail = 0usize;
    
    let mut head = 0usize;
    let mut tail = 0usize;
    
    println!("Phase 1: Producer fills buffer");
    for i in 0..capacity {
        let len = tail.wrapping_sub(producer_cached_head);
        println!("  Push {}: len={}, can push={}", i, len, len < capacity);
        tail += 1;
    }
    println!("  head={}, tail={}", head, tail);
    
    println!("\nPhase 2: Producer tries to push again");
    let len = tail.wrapping_sub(producer_cached_head);
    println!("  len={} >= capacity={}, BLOCKED", len, capacity);
    
    println!("\nPhase 3: Consumer pulls ONE item");
    // Consumer refreshes cached_tail
    consumer_cached_tail = tail;
    let was_full = consumer_cached_tail.wrapping_sub(head) >= capacity;
    println!("  was_full={}, wake={}", was_full, was_full);
    head += 1;
    
    println!("\nPhase 4: Producer retries");
    println!("  Producer still has cached_head={}", producer_cached_head);
    let len_stale = tail.wrapping_sub(producer_cached_head);
    println!("  With stale cache: len={} >= capacity={}, still blocked", len_stale, capacity);
    println!("  Producer refreshes cached_head");
    producer_cached_head = head;
    let len_fresh = tail.wrapping_sub(producer_cached_head);
    println!("  With fresh cache: len={} < capacity={}, CAN PUSH!", len_fresh, capacity);
    
    println!("\n=== Analysis ===");
    println!("The wake from was_full is CORRECT!");
    println!("It wakes the producer WHEN THE BUFFER WAS FULL.");
    println!("Producer wakes up, refreshes cached_head, and can proceed.");
    println!("\nThe next pull won't trigger wake (was_full=false),");
    println!("but that's fine - producer already got through!");
}
