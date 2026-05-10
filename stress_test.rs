use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;

// Minimal SPSC ring buffer for stress testing
struct RingBuffer<T> {
    slots: Vec<std::cell::UnsafeCell<std::mem::MaybeUninit<T>>>,
    capacity: usize,
    mask: usize,
    head: std::sync::atomic::AtomicUsize,
    tail: std::sync::atomic::AtomicUsize,
}

unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> RingBuffer<T> {
    fn new(capacity: usize) -> Arc<Self> {
        let size = (capacity + 1).next_power_of_two();
        let slots = (0..size).map(|_| {
            std::cell::UnsafeCell::new(std::mem::MaybeUninit::uninit())
        }).collect();
        
        Arc::new(Self {
            slots,
            capacity,
            mask: size - 1,
            head: std::sync::atomic::AtomicUsize::new(0),
            tail: std::sync::atomic::AtomicUsize::new(0),
        })
    }
    
    fn push(&self, cached_head: &mut usize, value: T) -> Result<(), T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let len = tail.wrapping_sub(*cached_head);
        if len >= self.capacity {
            *cached_head = self.head.load(Ordering::Acquire);
            let len = tail.wrapping_sub(*cached_head);
            if len >= self.capacity {
                return Err(value);
            }
        }
        let slot = tail & self.mask;
        unsafe {
            (*self.slots[slot].get()).write(value);
        }
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }
    
    fn pull(&self, cached_tail: &mut usize) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);
        if head == *cached_tail {
            *cached_tail = self.tail.load(Ordering::Acquire);
            if head == *cached_tail {
                return None;
            }
        }
        let slot = head & self.mask;
        let value = unsafe { (*self.slots[slot].get()).assume_init_read() };
        self.head.store(head.wrapping_add(1), Ordering::Release);
        Some(value)
    }
}

fn main() {
    println!("=== SPSC Ring Buffer Stress Test ===\n");
    
    let ring = RingBuffer::<usize>::new(4);
    let ring_prod = Arc::clone(&ring);
    let ring_cons = Arc::clone(&ring);
    let done = Arc::new(AtomicBool::new(false));
    let done_prod = Arc::clone(&done);
    
    let producer = thread::spawn(move || {
        let mut cached_head = 0;
        let mut pushed = 0usize;
        
        for i in 0..100000 {
            while let Err(_) = ring_prod.push(&mut cached_head, i) {
                thread::yield_now();
            }
            pushed += 1;
        }
        done_prod.store(true, Ordering::Release);
        println!("Producer: pushed {} items", pushed);
        pushed
    });
    
    let consumer = thread::spawn(move || {
        let mut cached_tail = 0;
        let mut pulled = 0usize;
        let mut last = None;
        
        loop {
            if let Some(value) = ring_cons.pull(&mut cached_tail) {
                if let Some(prev) = last {
                    if value != prev + 1 {
                        panic!("Out of order: got {}, expected {}", value, prev + 1);
                    }
                }
                last = Some(value);
                pulled += 1;
            } else if done.load(Ordering::Acquire) {
                // Final drain
                while let Some(value) = ring_cons.pull(&mut cached_tail) {
                    if let Some(prev) = last {
                        if value != prev + 1 {
                            panic!("Out of order: got {}, expected {}", value, prev + 1);
                        }
                    }
                    last = Some(value);
                    pulled += 1;
                }
                break;
            } else {
                thread::yield_now();
            }
        }
        
        println!("Consumer: pulled {} items", pulled);
        println!("Last value: {:?}", last);
        pulled
    });
    
    let pushed = producer.join().unwrap();
    let pulled = consumer.join().unwrap();
    
    assert_eq!(pushed, pulled, "Mismatch: pushed {} != pulled {}", pushed, pulled);
    println!("\nSUCCESS: All {} items transferred correctly!", pushed);
}
