fn main() {
    println!("Testing wrapping_sub with SPSC semantics:\n");
    
    let capacity = 4usize;
    
    // Case 1: Normal operation
    let head1 = 0usize;
    let tail1 = 3usize;
    let len1 = tail1.wrapping_sub(head1);
    println!("Case 1: head={}, tail={}, len={}, full?={}", 
             head1, tail1, len1, len1 >= capacity);
    
    // Case 2: Buffer full
    let head2 = 0usize;
    let tail2 = 4usize;
    let len2 = tail2.wrapping_sub(head2);
    println!("Case 2: head={}, tail={}, len={}, full?={}", 
             head2, tail2, len2, len2 >= capacity);
    
    // Case 3: Both wrapped
    let head3 = 100usize;
    let tail3 = 103usize;
    let len3 = tail3.wrapping_sub(head3);
    println!("Case 3: head={}, tail={}, len={}, full?={}", 
             head3, tail3, len3, len3 >= capacity);
    
    // Case 4: tail wraps before head (impossible in SPSC)
    let head4 = usize::MAX - 2;
    let tail4 = 1usize;
    let len4 = tail4.wrapping_sub(head4);
    println!("Case 4 INVALID: head={}, tail={}, len={}", head4, tail4, len4);
}
