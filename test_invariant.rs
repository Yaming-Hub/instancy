// The SPSC ring buffer maintains the invariant: tail is always >= head (modulo wrapping)
// The producer increments tail, consumer increments head
// So head can NEVER overtake tail in the actual execution

fn main() {
    // Scenario 1: Producer loads stale head
    // Real state: head=1 (consumer consumed), tail=0 (producer hasn't added)
    // Producer cached_head=0 (stale)
    
    let tail = 0usize;
    let real_head = 1usize;
    let cached_head = 0usize;
    
    // Producer computes len with stale cache
    let len_stale = tail.wrapping_sub(cached_head);
    println!("Producer with stale cache: tail={}, cached_head={}, len={}", 
             tail, cached_head, len_stale);
    
    // Producer refreshes when it sees buffer full
    let len_fresh = tail.wrapping_sub(real_head);
    println!("After refresh: tail={}, real_head={}, len={}", 
             tail, real_head, len_fresh);
    
    // Wait - if tail=0 and head=1, that means head > tail
    // But the invariant is: in SPSC, tail is ALWAYS the next write position
    // and head is ALWAYS the next read position
    // So if head > tail, the buffer must have wrapped around!
    
    println!("\nActually, this scenario is impossible in correct SPSC:");
    println!("- Producer only writes at tail, then increments tail");
    println!("- Consumer only reads at head (when head != tail), then increments head");
    println!("- So head can equal tail (empty) or be behind tail, but never ahead");
    
    println!("\nThe only way head > tail is if the indices have wrapped.");
    println!("Example: capacity=4, head=1, tail=0 after many wraps");
    println!("This represents: head has wrapped around and is 1 position behind tail");
    println!("wrapping_sub gives: {}", (0usize).wrapping_sub(1));
    println!("Which is nearly usize::MAX, and >= capacity, so producer blocks (correct!)");
}
