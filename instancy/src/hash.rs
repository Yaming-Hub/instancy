//! Stable, platform-independent hashing.
//!
//! [`std::collections::hash_map::DefaultHasher`] is explicitly *not* stable:
//! its algorithm may change between Rust releases, and `usize`/`isize` are fed
//! to it at native width, so the same key can hash differently on 32- and
//! 64-bit targets. Anything that must agree across the nodes of a cluster —
//! the worker a key is routed to by [`Pipe::exchange`](crate::Pipe::exchange),
//! or the graph fingerprint compared during the cluster handshake — uses
//! [`StableHasher`] instead.
//!
//! # Algorithm
//!
//! The hasher keeps one 64-bit state, initialised to [`StableHasher::SEED`].
//! Every integer write is widened to 64 bits (`usize`/`isize` are always
//! treated as `u64`/`i64`, and `u128`/`i128` are fed as two little-endian
//! words, low word first) and mixed in with
//!
//! ```text
//! state = ((state ^ word) * 0x9E37_79B9_7F4A_7C15).rotate_left(29) + 0x632B_E59B_D9B4_E019
//! ```
//!
//! using wrapping arithmetic. A byte slice is fed as little-endian 8-byte
//! words, then its zero-padded tail (if any), then its length.
//! [`finish`](std::hash::Hasher::finish) applies the MurmurHash3 64-bit
//! finaliser to the state. These rules are part of the crate's wire contract
//! and only change together with the cluster protocol version.
//!
//! `StableHasher` is not designed to resist hash flooding and must not be used
//! for hash tables keyed by untrusted input.
//!
//! # Stability of `Hash` implementations
//!
//! The hasher fixes how written integers and bytes are combined, but the
//! sequence of writes comes from each type's [`Hash`] implementation.
//! Single integers, `bool`, `char`, `str`/`String`, and tuples or
//! `#[derive(Hash)]` structs of those produce the same writes on every
//! platform. **Slices, arrays and `Vec`s of multi-byte integers do not:** the
//! standard library hashes them as one block of raw native-endian bytes, so
//! keys such as `[u64; 2]` or `Vec<u32>` only route identically between nodes
//! with the same byte order, and `[usize; N]` / `Vec<isize>` additionally need
//! the same pointer width. The standard library also does not formally
//! guarantee these encodings across Rust releases. For such keys, convert to a
//! tuple or struct of fixed-width integers, or use
//! [`Pipe::exchange_by_hash`](crate::Pipe::exchange_by_hash) with your own
//! `u64` key function.

use std::hash::{Hash, Hasher};

/// A fast 64-bit hasher whose output is identical on every platform, Rust
/// release, and process.
///
/// See the [module documentation](self) for the exact algorithm.
///
/// # Example
///
/// ```
/// use std::hash::{Hash, Hasher};
/// use instancy::hash::{StableHasher, stable_hash};
///
/// let mut h = StableHasher::new();
/// 42u64.hash(&mut h);
/// assert_eq!(h.finish(), stable_hash(&42u64));
/// // usize is always hashed as u64, so this holds on 32- and 64-bit targets.
/// assert_eq!(stable_hash(&42usize), stable_hash(&42u64));
/// ```
#[derive(Debug, Clone)]
pub struct StableHasher {
    state: u64,
}

impl StableHasher {
    /// Initial state of every new hasher.
    pub const SEED: u64 = 0x243F_6A88_85A3_08D3;

    const MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;
    const INCREMENT: u64 = 0x632B_E59B_D9B4_E019;

    /// Create a hasher in its initial state.
    pub const fn new() -> Self {
        Self { state: Self::SEED }
    }

    #[inline]
    fn mix(&mut self, word: u64) {
        self.state = (self.state ^ word)
            .wrapping_mul(Self::MULTIPLIER)
            .rotate_left(29)
            .wrapping_add(Self::INCREMENT);
    }
}

impl Default for StableHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher for StableHasher {
    #[inline]
    fn finish(&self) -> u64 {
        // MurmurHash3 fmix64.
        let mut h = self.state;
        h ^= h >> 33;
        h = h.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        h ^= h >> 33;
        h = h.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
        h ^= h >> 33;
        h
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            let mut word = [0u8; 8];
            word.copy_from_slice(chunk);
            self.mix(u64::from_le_bytes(word));
        }
        let tail = chunks.remainder();
        if !tail.is_empty() {
            let mut word = [0u8; 8];
            word[..tail.len()].copy_from_slice(tail);
            self.mix(u64::from_le_bytes(word));
        }
        self.mix(bytes.len() as u64);
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.mix(u64::from(i));
    }

    #[inline]
    fn write_u16(&mut self, i: u16) {
        self.mix(u64::from(i));
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.mix(u64::from(i));
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.mix(i);
    }

    #[inline]
    fn write_u128(&mut self, i: u128) {
        self.mix(i as u64);
        self.mix((i >> 64) as u64);
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.mix(i as u64);
    }

    #[inline]
    fn write_isize(&mut self, i: isize) {
        self.mix(i as i64 as u64);
    }
}

/// Hash a value with a fresh [`StableHasher`].
///
/// This is the hash [`Pipe::exchange`](crate::Pipe::exchange) applies to the
/// key returned by its key function before reducing it modulo the worker
/// count.
#[inline]
pub fn stable_hash<K: Hash + ?Sized>(key: &K) -> u64 {
    let mut hasher = StableHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Golden values: these pin the algorithm. Changing any of them changes
    /// cross-node key routing and requires a cluster protocol version bump.
    #[test]
    fn golden_values_are_stable() {
        let bytes = |b: &[u8]| {
            let mut h = StableHasher::new();
            h.write(b);
            h.finish()
        };
        let golden: [(u64, u64); 6] = [
            (stable_hash(&0u64), 0x0556_c50f_3fa4_5ac8),
            (stable_hash(&1u64), 0xd04c_a76a_c679_b738),
            (stable_hash(&u64::MAX), 0x51b4_6eca_50e2_21ce),
            (stable_hash(&(1u64, 2u64)), 0xb1b8_1cb6_1704_b78e),
            (bytes(b"instancy"), 0x8467_800a_0ad5_84a1),
            (bytes(b"cluster-protocol"), 0xd3eb_7641_77de_165c),
        ];
        for (actual, expected) in golden {
            assert_eq!(actual, expected, "{actual:#018x} != {expected:#018x}");
        }
    }

    #[test]
    fn integer_widths_are_platform_independent() {
        assert_eq!(stable_hash(&7usize), stable_hash(&7u64));
        assert_eq!(stable_hash(&-7isize), stable_hash(&-7i64));
        assert_eq!(stable_hash(&7u32), stable_hash(&7u64));
        assert_eq!(stable_hash(&7u8), stable_hash(&7u64));
    }

    #[test]
    fn u128_is_two_little_endian_words() {
        let v: u128 = (5u128 << 64) | 9;
        let mut a = StableHasher::new();
        a.write_u128(v);
        let mut b = StableHasher::new();
        b.write_u64(9);
        b.write_u64(5);
        assert_eq!(a.finish(), b.finish());
    }

    #[test]
    fn byte_writes_are_length_delimited() {
        let mut a = StableHasher::new();
        a.write(b"ab");
        a.write(b"c");
        let mut b = StableHasher::new();
        b.write(b"a");
        b.write(b"bc");
        assert_ne!(a.finish(), b.finish());

        let mut empty = StableHasher::new();
        empty.write(b"");
        assert_ne!(empty.finish(), StableHasher::new().finish());

        // A 7-byte write must differ from an 8-byte write that ends in 7.
        let mut seven = StableHasher::new();
        seven.write(&[1, 2, 3, 4, 5, 6, 7]);
        let mut eight = StableHasher::new();
        eight.write(&[1, 2, 3, 4, 5, 6, 7, 7]);
        assert_ne!(seven.finish(), eight.finish());
    }

    #[test]
    fn write_order_matters() {
        assert_ne!(stable_hash(&(1u64, 2u64)), stable_hash(&(2u64, 1u64)));
    }

    #[test]
    fn sequential_keys_spread_evenly_across_workers() {
        let patterns: [fn(u64, u64) -> u64; 4] =
            [|k, _| k, |k, w| k * w, |k, _| k << 32, |k, _| k * 1000];
        for pattern in patterns {
            for workers in [2u64, 3, 4, 7, 16, 100] {
                let n = 100_000u64;
                let mut counts = vec![0u64; workers as usize];
                for k in 0..n {
                    counts[(stable_hash(&pattern(k, workers)) % workers) as usize] += 1;
                }
                let expected = n / workers;
                // Binomial standard deviation; 5 sigma keeps the test
                // deterministic while still catching a biased or clustered hash.
                let sigma = (expected as f64).sqrt();
                for c in counts {
                    let dev = (c as f64 - expected as f64).abs();
                    assert!(
                        dev < 5.0 * sigma,
                        "workers={workers}: bucket {c} vs {expected}"
                    );
                }
            }
        }
    }
}
