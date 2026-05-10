// Detailed trace of pull() with cached_tail

fn main() {
    println!("=== Tracing pull() execution ===\n");
    
    // Initial state
    let mut head = 0usize;
    let mut cached_tail = 0usize;
    let real_tail = 4usize;
    let capacity = 4usize;
    
    println!("Initial: head={}, cached_tail={}, real_tail={}", head, cached_tail, real_tail);
    
    println!("\n--- First pull() ---");
    println!("1. Load head (Relaxed): {}", head);
    println!("2. Compare head == cached_tail: {} == {} -> {}", head, cached_tail, head == cached_tail);
    println!("3. YES, refresh cached_tail from tail (Acquire)");
    cached_tail = real_tail;
    println!("   cached_tail = {}", cached_tail);
    println!("4. Compare head == cached_tail: {} == {} -> {}", head, cached_tail, head == cached_tail);
    println!("5. NO, proceed to read slot[{}]", head);
    
    let len = cached_tail.wrapping_sub(head);
    let was_full = len >= capacity;
    println!("6. Check was_full: len={}, capacity={}, was_full={}", len, capacity, was_full);
    println!("7. Store head.wrapping_add(1)={} with Release", head + 1);
    head += 1;
    println!("8. Wake producer because was_full={}", was_full);
    
    println!("\n--- Second pull() ---");
    println!("1. Load head (Relaxed): {}", head);
    println!("2. Compare head == cached_tail: {} == {} -> {}", head, cached_tail, head == cached_tail);
    println!("3. NO, skip refresh. cached_tail={} (stale? no, was just refreshed)", cached_tail);
    println!("4. Read slot[{}]", head);
    
    let len2 = cached_tail.wrapping_sub(head);
    let was_full2 = len2 >= capacity;
    println!("5. Check was_full: len={}, capacity={}, was_full={}", len2, capacity, was_full2);
    println!("6. Store head.wrapping_add(1)={} with Release", head + 1);
    head += 1;
    println!("7. was_full={}, so {} wake", was_full2, if was_full2 { "DO" } else { "NO" });
    
    println!("\n--- Scenario where missed wake could occur ---");
    println!("Consumer starts: head=0, cached_tail=0");
    println!("Producer fills to capacity: real_tail=4");
    println!("Consumer pull #1: refreshes cached_tail=4, sees was_full=true, wakes");
    println!("Consumer pull #2: cached_tail=4, head=1, len=3, was_full=false, no wake");
    println!("Producer tries to push: len=4 >= 4, BLOCKED");
    println!("\nProducer is blocked but consumer didn't wake!");
    println!("However, consumer just pulled twice, so producer can push soon anyway.");
}
