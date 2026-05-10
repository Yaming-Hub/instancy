fn main() {
    let max = usize::MAX;
    let wrapped = max.wrapping_add(1);
    println!("usize::MAX = {}", max);
    println!("usize::MAX.wrapping_add(1) = {}", wrapped);
    
    // Simulate many operations
    let mut idx: usize = usize::MAX - 10;
    for _ in 0..20 {
        idx = idx.wrapping_add(1);
        let masked = idx & 7; // Power of 2 mask
        println!("idx={}, masked={}", idx, masked);
    }
}
