//! Exchange key-routing hash benchmark.
//!
//! `Pipe::exchange` hashes the key of every record to pick its target worker,
//! so the hash is on the exchange hot path. This compares the stable hasher
//! used since cluster protocol version 2 with the `DefaultHasher` (SipHash-1-3)
//! it replaced, over typical key shapes.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use instancy::hash::StableHasher;

const KEYS: usize = 4096;

fn route<H: Hasher + Default, K: Hash>(keys: &[K], workers: u64) -> u64 {
    let mut acc = 0u64;
    for key in keys {
        let mut h = H::default();
        key.hash(&mut h);
        acc = acc.wrapping_add(h.finish() % workers);
    }
    acc
}

fn bench_keys<K: Hash>(c: &mut Criterion, shape: &str, keys: &[K]) {
    let mut group = c.benchmark_group("exchange_key_hash");
    group.throughput(Throughput::Elements(keys.len() as u64));
    group.bench_with_input(BenchmarkId::new("stable", shape), keys, |b, keys| {
        b.iter(|| route::<StableHasher, K>(black_box(keys), 8))
    });
    group.bench_with_input(
        BenchmarkId::new("default_siphash", shape),
        keys,
        |b, keys| b.iter(|| route::<DefaultHasher, K>(black_box(keys), 8)),
    );
    group.finish();
}

fn bench_exchange_hash(c: &mut Criterion) {
    let u64_keys: Vec<u64> = (0..KEYS as u64).map(|k| k.wrapping_mul(7919)).collect();
    bench_keys(c, "u64", &u64_keys);

    let pair_keys: Vec<(u64, u32)> = (0..KEYS as u64).map(|k| (k, k as u32 ^ 0xA5)).collect();
    bench_keys(c, "u64_u32_tuple", &pair_keys);

    let string_keys: Vec<String> = (0..KEYS).map(|k| format!("user-{k:010}")).collect();
    bench_keys(c, "string_16b", &string_keys);
}

criterion_group!(benches, bench_exchange_hash);
criterion_main!(benches);
