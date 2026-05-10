//! Safe byte-parsing helpers for wire-format decoding.
//!
//! These replace `.try_into().expect("X is N bytes")` patterns throughout
//! the networking and progress code, providing proper error propagation
//! instead of panicking on malformed input.

use crate::Error;

/// Read a `u32` in little-endian from `buf` at `offset`.
pub(crate) fn read_u32(buf: &[u8], offset: usize) -> Result<u32, Error> {
    let end = offset.saturating_add(4);
    buf.get(offset..end)
        .and_then(|s| <[u8; 4]>::try_from(s).ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| {
            Error::Codec(Box::new(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "expected 4 bytes at offset {offset}, got {}",
                    buf.len().saturating_sub(offset)
                ),
            )))
        })
}

/// Read a `u64` in little-endian from `buf` at `offset`.
pub(crate) fn read_u64(buf: &[u8], offset: usize) -> Result<u64, Error> {
    let end = offset.saturating_add(8);
    buf.get(offset..end)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .map(u64::from_le_bytes)
        .ok_or_else(|| {
            Error::Codec(Box::new(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "expected 8 bytes at offset {offset}, got {}",
                    buf.len().saturating_sub(offset)
                ),
            )))
        })
}

/// Read an `i64` in little-endian from `buf` at `offset`.
pub(crate) fn read_i64(buf: &[u8], offset: usize) -> Result<i64, Error> {
    let end = offset.saturating_add(8);
    buf.get(offset..end)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .map(i64::from_le_bytes)
        .ok_or_else(|| {
            Error::Codec(Box::new(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "expected 8 bytes at offset {offset}, got {}",
                    buf.len().saturating_sub(offset)
                ),
            )))
        })
}

/// Read a fixed-size byte array from `buf` at `offset`.
pub(crate) fn read_array<const N: usize>(buf: &[u8], offset: usize) -> Result<[u8; N], Error> {
    let end = offset.saturating_add(N);
    buf.get(offset..end)
        .and_then(|s| <[u8; N]>::try_from(s).ok())
        .ok_or_else(|| {
            Error::Codec(Box::new(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "expected {N} bytes at offset {offset}, got {}",
                    buf.len().saturating_sub(offset)
                ),
            )))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_u32_ok() {
        let buf = 42u32.to_le_bytes();
        assert_eq!(read_u32(&buf, 0).unwrap(), 42);
    }

    #[test]
    fn read_u32_offset() {
        let mut buf = vec![0xFF; 4];
        buf.extend_from_slice(&99u32.to_le_bytes());
        assert_eq!(read_u32(&buf, 4).unwrap(), 99);
    }

    #[test]
    fn read_u32_truncated() {
        let buf = [0u8; 3];
        assert!(read_u32(&buf, 0).is_err());
    }

    #[test]
    fn read_u32_out_of_bounds() {
        let buf = [0u8; 4];
        assert!(read_u32(&buf, 2).is_err());
    }

    #[test]
    fn read_u64_ok() {
        let buf = 123456789u64.to_le_bytes();
        assert_eq!(read_u64(&buf, 0).unwrap(), 123456789);
    }

    #[test]
    fn read_u64_truncated() {
        let buf = [0u8; 7];
        assert!(read_u64(&buf, 0).is_err());
    }

    #[test]
    fn read_i64_ok() {
        let buf = (-42i64).to_le_bytes();
        assert_eq!(read_i64(&buf, 0).unwrap(), -42);
    }

    #[test]
    fn read_array_ok() {
        let buf = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let arr: [u8; 16] = read_array(&buf, 0).unwrap();
        assert_eq!(arr, buf);
    }

    #[test]
    fn read_array_truncated() {
        let buf = [0u8; 10];
        assert!(read_array::<16>(&buf, 0).is_err());
    }

    #[test]
    fn read_empty_buffer() {
        assert!(read_u32(&[], 0).is_err());
        assert!(read_u64(&[], 0).is_err());
        assert!(read_i64(&[], 0).is_err());
        assert!(read_array::<4>(&[], 0).is_err());
    }
}
