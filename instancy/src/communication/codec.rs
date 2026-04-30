//! Pluggable serialization for inter-process data exchange.
//!
//! This module defines the [`Codec`] trait for encoding/decoding typed data to/from
//! bytes, enabling customized serialization strategies for network transport.
//!
//! # Design
//!
//! - [`Codec<T>`] is the core trait: `encode` serializes a value into a byte buffer,
//!   `decode` deserializes from a byte slice.
//! - [`BincodeCodec`] provides a default implementation using the `bincode` + `serde`
//!   ecosystem (behind the `bincode-codec` feature flag).
//! - Users can implement custom codecs for domain-specific formats (protobuf,
//!   flatbuffers, zero-copy, etc.).
//!
//! # Data traits
//!
//! - [`Data`] — the minimum bound for intra-process data: `Clone + Send + 'static`.
//! - [`ExchangeData`] — data that can cross process boundaries: `Data + Codec support`.
//!
//! # Example: Custom codec
//!
//! ```
//! use instancy::communication::codec::{Codec, CodecError};
//!
//! struct LengthPrefixedStringCodec;
//!
//! impl Codec<String> for LengthPrefixedStringCodec {
//!     fn encode(&self, value: &String, buf: &mut Vec<u8>) -> Result<(), CodecError> {
//!         let len = value.len() as u32;
//!         buf.extend_from_slice(&len.to_le_bytes());
//!         buf.extend_from_slice(value.as_bytes());
//!         Ok(())
//!     }
//!
//!     fn decode(&self, buf: &[u8]) -> Result<(String, usize), CodecError> {
//!         if buf.len() < 4 {
//!             return Err(CodecError::InsufficientData {
//!                 needed: 4,
//!                 available: buf.len(),
//!             });
//!         }
//!         let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
//!         if buf.len() < 4 + len {
//!             return Err(CodecError::InsufficientData {
//!                 needed: 4 + len,
//!                 available: buf.len(),
//!             });
//!         }
//!         let s = String::from_utf8(buf[4..4 + len].to_vec())
//!             .map_err(|e| CodecError::InvalidData(e.to_string()))?;
//!         Ok((s, 4 + len))
//!     }
//! }
//! ```

use std::fmt;
use std::marker::PhantomData;

/// Maximum allowed message size (256 MB). Prevents allocation-based DoS from
/// malicious or corrupted length prefixes.
pub const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

/// Errors that can occur during encoding or decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// Not enough bytes available to decode a value.
    InsufficientData {
        /// Minimum bytes needed.
        needed: usize,
        /// Bytes actually available.
        available: usize,
    },
    /// The data is malformed or cannot be interpreted.
    InvalidData(String),
    /// A custom error from a user-defined codec.
    Custom(String),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientData { needed, available } => {
                write!(f, "insufficient data: need {needed} bytes, have {available}")
            }
            Self::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            Self::Custom(msg) => write!(f, "codec error: {msg}"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Encode a length as a 4-byte little-endian prefix. Returns error if value exceeds u32 or MAX_MESSAGE_SIZE.
fn encode_length_prefix(len: usize, buf: &mut Vec<u8>) -> Result<(), CodecError> {
    if len > MAX_MESSAGE_SIZE {
        return Err(CodecError::Custom(format!(
            "payload too large: {len} bytes exceeds maximum {MAX_MESSAGE_SIZE}"
        )));
    }
    let len_u32 = u32::try_from(len).map_err(|_| {
        CodecError::Custom(format!("payload too large for u32 length prefix: {len} bytes"))
    })?;
    buf.extend_from_slice(&len_u32.to_le_bytes());
    Ok(())
}

/// Decode a 4-byte little-endian length prefix. Validates against MAX_MESSAGE_SIZE.
fn decode_length_prefix(buf: &[u8]) -> Result<usize, CodecError> {
    if buf.len() < 4 {
        return Err(CodecError::InsufficientData {
            needed: 4,
            available: buf.len(),
        });
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len > MAX_MESSAGE_SIZE {
        return Err(CodecError::InvalidData(format!(
            "length prefix {len} exceeds maximum message size {MAX_MESSAGE_SIZE}"
        )));
    }
    Ok(len)
}

/// Trait for encoding and decoding typed values to/from byte buffers.
///
/// A codec is stateless and reusable — it can encode/decode many values.
/// Implementations must be `Send + Sync` to allow sharing across threads.
///
/// # Protocol
///
/// - `encode` appends the serialized representation to the provided `Vec<u8>`.
///   It must not clear or truncate the buffer (caller may be batching multiple values).
/// - `decode` reads from the beginning of the slice and returns the decoded value
///   plus the number of bytes consumed. This allows the caller to advance through
///   a buffer containing multiple encoded values.
pub trait Codec<T>: Send + Sync {
    /// Encode a value, appending bytes to `buf`.
    fn encode(&self, value: &T, buf: &mut Vec<u8>) -> Result<(), CodecError>;

    /// Decode a value from the beginning of `buf`.
    ///
    /// Returns the decoded value and the number of bytes consumed.
    fn decode(&self, buf: &[u8]) -> Result<(T, usize), CodecError>;
}

/// Marker trait for data types that can be used within a single process.
///
/// This is the minimum bound for data flowing between operators on the same node.
/// No serialization is required — data is passed by cloning through in-process channels.
pub trait Data: Clone + Send + 'static {}

/// Blanket implementation: any type that is `Clone + Send + 'static` is `Data`.
impl<T: Clone + Send + 'static> Data for T {}

/// Trait for data types that can cross process boundaries via serialization.
///
/// `ExchangeData` extends [`Data`] with an associated codec type. Any type that
/// needs to be sent over the network must implement this trait (or use the
/// blanket implementation provided by the `bincode-codec` feature).
pub trait ExchangeData: Data {
    /// The codec type used to serialize/deserialize this data.
    type CodecType: Codec<Self>;

    /// Create a new codec instance for this type.
    fn codec() -> Self::CodecType;
}

// =============================================================================
// Bincode codec (behind feature flag)
// =============================================================================

/// A codec implementation using `bincode` serialization.
///
/// Available when the `bincode-codec` feature is enabled. Works with any type
/// that implements `serde::Serialize + serde::DeserializeOwned`.
#[cfg(feature = "bincode-codec")]
pub struct BincodeCodec<T> {
    _phantom: PhantomData<T>,
}

#[cfg(feature = "bincode-codec")]
impl<T> BincodeCodec<T> {
    /// Create a new BincodeCodec instance.
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

#[cfg(feature = "bincode-codec")]
impl<T> Default for BincodeCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "bincode-codec")]
impl<T> fmt::Debug for BincodeCodec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BincodeCodec").finish()
    }
}

#[cfg(feature = "bincode-codec")]
impl<T> Clone for BincodeCodec<T> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

#[cfg(feature = "bincode-codec")]
impl<T> Codec<T> for BincodeCodec<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    fn encode(&self, value: &T, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        let payload = bincode::serialize(value)
            .map_err(|e| CodecError::Custom(format!("bincode encode: {e}")))?;
        encode_length_prefix(payload.len(), buf)?;
        buf.extend_from_slice(&payload);
        Ok(())
    }

    fn decode(&self, buf: &[u8]) -> Result<(T, usize), CodecError> {
        let len = decode_length_prefix(buf)?;
        let total = 4 + len;
        if buf.len() < total {
            return Err(CodecError::InsufficientData {
                needed: total,
                available: buf.len(),
            });
        }
        let value = bincode::deserialize(&buf[4..total])
            .map_err(|e| CodecError::Custom(format!("bincode decode: {e}")))?;
        Ok((value, total))
    }
}

// =============================================================================
// Identity codec (for types that are already byte buffers)
// =============================================================================

/// A no-op codec for `Vec<u8>` — stores raw bytes with a length prefix.
///
/// Useful for pre-serialized data or opaque binary payloads.
#[derive(Debug, Clone, Copy)]
pub struct RawBytesCodec;

impl Codec<Vec<u8>> for RawBytesCodec {
    fn encode(&self, value: &Vec<u8>, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        encode_length_prefix(value.len(), buf)?;
        buf.extend_from_slice(value);
        Ok(())
    }

    fn decode(&self, buf: &[u8]) -> Result<(Vec<u8>, usize), CodecError> {
        let len = decode_length_prefix(buf)?;
        let total = 4 + len;
        if buf.len() < total {
            return Err(CodecError::InsufficientData {
                needed: total,
                available: buf.len(),
            });
        }
        Ok((buf[4..total].to_vec(), total))
    }
}

/// Implement `ExchangeData` for `Vec<u8>` using `RawBytesCodec`.
impl ExchangeData for Vec<u8> {
    type CodecType = RawBytesCodec;
    fn codec() -> Self::CodecType {
        RawBytesCodec
    }
}

// =============================================================================
// Fixed-size codec for primitives
// =============================================================================

/// A codec for fixed-size types that are `Copy` and have a known byte representation.
///
/// Uses native-endian (little-endian on most platforms) encoding with no length prefix
/// since the size is known at compile time.
#[derive(Debug, Clone, Copy)]
pub struct FixedSizeCodec<T: Copy> {
    _phantom: PhantomData<T>,
}

impl<T: Copy> FixedSizeCodec<T> {
    /// Create a new FixedSizeCodec.
    pub const fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

macro_rules! impl_fixed_codec {
    ($($ty:ty),+) => {
        $(
            impl Codec<$ty> for FixedSizeCodec<$ty> {
                fn encode(&self, value: &$ty, buf: &mut Vec<u8>) -> Result<(), CodecError> {
                    buf.extend_from_slice(&value.to_le_bytes());
                    Ok(())
                }

                fn decode(&self, buf: &[u8]) -> Result<($ty, usize), CodecError> {
                    const SIZE: usize = std::mem::size_of::<$ty>();
                    if buf.len() < SIZE {
                        return Err(CodecError::InsufficientData {
                            needed: SIZE,
                            available: buf.len(),
                        });
                    }
                    // SAFETY: length check above guarantees buf[..SIZE] is exactly SIZE bytes
                    let bytes: [u8; SIZE] = buf[..SIZE].try_into()
                        .map_err(|_| CodecError::InvalidData("slice length mismatch".into()))?;
                    let value = <$ty>::from_le_bytes(bytes);
                    Ok((value, SIZE))
                }
            }

            impl ExchangeData for $ty {
                type CodecType = FixedSizeCodec<$ty>;
                fn codec() -> Self::CodecType {
                    FixedSizeCodec::new()
                }
            }
        )+
    };
}

impl_fixed_codec!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);

// =============================================================================
// String codec (length-prefixed UTF-8)
// =============================================================================

/// A codec for `String` values using length-prefixed UTF-8 encoding.
#[derive(Debug, Clone, Copy)]
pub struct StringCodec;

impl Codec<String> for StringCodec {
    fn encode(&self, value: &String, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        encode_length_prefix(value.len(), buf)?;
        buf.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn decode(&self, buf: &[u8]) -> Result<(String, usize), CodecError> {
        let len = decode_length_prefix(buf)?;
        let total = 4 + len;
        if buf.len() < total {
            return Err(CodecError::InsufficientData {
                needed: total,
                available: buf.len(),
            });
        }
        let s = String::from_utf8(buf[4..total].to_vec())
            .map_err(|e| CodecError::InvalidData(format!("invalid UTF-8: {e}")))?;
        Ok((s, total))
    }
}

impl ExchangeData for String {
    type CodecType = StringCodec;
    fn codec() -> Self::CodecType {
        StringCodec
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- CodecError tests ---

    #[test]
    fn codec_error_display() {
        let e = CodecError::InsufficientData {
            needed: 10,
            available: 3,
        };
        assert_eq!(e.to_string(), "insufficient data: need 10 bytes, have 3");

        let e = CodecError::InvalidData("bad utf8".into());
        assert_eq!(e.to_string(), "invalid data: bad utf8");

        let e = CodecError::Custom("something broke".into());
        assert_eq!(e.to_string(), "codec error: something broke");
    }

    // --- FixedSizeCodec tests ---

    #[test]
    fn fixed_codec_u32_roundtrip() {
        let codec = FixedSizeCodec::<u32>::new();
        let mut buf = Vec::new();
        codec.encode(&42u32, &mut buf).unwrap();
        assert_eq!(buf.len(), 4);
        let (val, consumed) = codec.decode(&buf).unwrap();
        assert_eq!(val, 42u32);
        assert_eq!(consumed, 4);
    }

    #[test]
    fn fixed_codec_u64_roundtrip() {
        let codec = FixedSizeCodec::<u64>::new();
        let mut buf = Vec::new();
        codec.encode(&123456789u64, &mut buf).unwrap();
        let (val, consumed) = codec.decode(&buf).unwrap();
        assert_eq!(val, 123456789u64);
        assert_eq!(consumed, 8);
    }

    #[test]
    fn fixed_codec_f64_roundtrip() {
        let codec = FixedSizeCodec::<f64>::new();
        let mut buf = Vec::new();
        codec.encode(&3.14159f64, &mut buf).unwrap();
        let (val, consumed) = codec.decode(&buf).unwrap();
        assert_eq!(val, 3.14159f64);
        assert_eq!(consumed, 8);
    }

    #[test]
    fn fixed_codec_i32_negative() {
        let codec = FixedSizeCodec::<i32>::new();
        let mut buf = Vec::new();
        codec.encode(&-999i32, &mut buf).unwrap();
        let (val, _) = codec.decode(&buf).unwrap();
        assert_eq!(val, -999i32);
    }

    #[test]
    fn fixed_codec_insufficient_data() {
        let codec = FixedSizeCodec::<u64>::new();
        let buf = vec![1, 2, 3]; // only 3 bytes, need 8
        let err = codec.decode(&buf).unwrap_err();
        assert_eq!(
            err,
            CodecError::InsufficientData {
                needed: 8,
                available: 3
            }
        );
    }

    #[test]
    fn fixed_codec_multiple_values_in_buffer() {
        let codec = FixedSizeCodec::<u32>::new();
        let mut buf = Vec::new();
        codec.encode(&10u32, &mut buf).unwrap();
        codec.encode(&20u32, &mut buf).unwrap();
        codec.encode(&30u32, &mut buf).unwrap();

        assert_eq!(buf.len(), 12);

        let (v1, c1) = codec.decode(&buf).unwrap();
        let (v2, c2) = codec.decode(&buf[c1..]).unwrap();
        let (v3, c3) = codec.decode(&buf[c1 + c2..]).unwrap();

        assert_eq!((v1, v2, v3), (10, 20, 30));
        assert_eq!(c1 + c2 + c3, 12);
    }

    // --- StringCodec tests ---

    #[test]
    fn string_codec_roundtrip() {
        let codec = StringCodec;
        let mut buf = Vec::new();
        let original = String::from("hello world");
        codec.encode(&original, &mut buf).unwrap();
        let (decoded, consumed) = codec.decode(&buf).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(consumed, 4 + 11); // 4-byte length + 11 chars
    }

    #[test]
    fn string_codec_empty_string() {
        let codec = StringCodec;
        let mut buf = Vec::new();
        let original = String::new();
        codec.encode(&original, &mut buf).unwrap();
        let (decoded, consumed) = codec.decode(&buf).unwrap();
        assert_eq!(decoded, "");
        assert_eq!(consumed, 4); // just the length prefix
    }

    #[test]
    fn string_codec_unicode() {
        let codec = StringCodec;
        let mut buf = Vec::new();
        let original = String::from("日本語テスト 🎉");
        codec.encode(&original, &mut buf).unwrap();
        let (decoded, _) = codec.decode(&buf).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn string_codec_insufficient_data_header() {
        let codec = StringCodec;
        let buf = vec![1, 2]; // only 2 bytes, need 4 for length prefix
        let err = codec.decode(&buf).unwrap_err();
        assert_eq!(
            err,
            CodecError::InsufficientData {
                needed: 4,
                available: 2
            }
        );
    }

    #[test]
    fn string_codec_insufficient_data_body() {
        let codec = StringCodec;
        // Length says 100 bytes, but we only have 10 total
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 6]); // only 6 payload bytes
        let err = codec.decode(&buf).unwrap_err();
        assert_eq!(
            err,
            CodecError::InsufficientData {
                needed: 104,
                available: 10
            }
        );
    }

    #[test]
    fn string_codec_invalid_utf8() {
        let codec = StringCodec;
        // Craft a buffer with valid length but invalid UTF-8
        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_le_bytes());
        buf.extend_from_slice(&[0xFF, 0xFE, 0xFD]); // invalid UTF-8
        let err = codec.decode(&buf).unwrap_err();
        match err {
            CodecError::InvalidData(msg) => assert!(msg.contains("invalid UTF-8")),
            other => panic!("expected InvalidData, got {other:?}"),
        }
    }

    // --- RawBytesCodec tests ---

    #[test]
    fn raw_bytes_codec_roundtrip() {
        let codec = RawBytesCodec;
        let mut buf = Vec::new();
        let original = vec![1u8, 2, 3, 4, 5];
        codec.encode(&original, &mut buf).unwrap();
        let (decoded, consumed) = codec.decode(&buf).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(consumed, 4 + 5);
    }

    #[test]
    fn raw_bytes_codec_empty() {
        let codec = RawBytesCodec;
        let mut buf = Vec::new();
        let original: Vec<u8> = vec![];
        codec.encode(&original, &mut buf).unwrap();
        let (decoded, consumed) = codec.decode(&buf).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(consumed, 4);
    }

    // --- ExchangeData trait tests ---

    #[test]
    fn exchange_data_u32() {
        let codec = u32::codec();
        let mut buf = Vec::new();
        codec.encode(&42u32, &mut buf).unwrap();
        let (val, _) = codec.decode(&buf).unwrap();
        assert_eq!(val, 42u32);
    }

    #[test]
    fn exchange_data_string() {
        let codec = String::codec();
        let mut buf = Vec::new();
        let s = String::from("exchange test");
        codec.encode(&s, &mut buf).unwrap();
        let (val, _) = codec.decode(&buf).unwrap();
        assert_eq!(val, s);
    }

    #[test]
    fn exchange_data_vec_u8() {
        let codec = Vec::<u8>::codec();
        let mut buf = Vec::new();
        let data = vec![10u8, 20, 30];
        codec.encode(&data, &mut buf).unwrap();
        let (val, _) = codec.decode(&buf).unwrap();
        assert_eq!(val, data);
    }

    // --- Custom codec test ---

    struct LengthPrefixedU16Codec;

    impl Codec<Vec<u16>> for LengthPrefixedU16Codec {
        fn encode(&self, value: &Vec<u16>, buf: &mut Vec<u8>) -> Result<(), CodecError> {
            let count = value.len() as u32;
            buf.extend_from_slice(&count.to_le_bytes());
            for &item in value {
                buf.extend_from_slice(&item.to_le_bytes());
            }
            Ok(())
        }

        fn decode(&self, buf: &[u8]) -> Result<(Vec<u16>, usize), CodecError> {
            if buf.len() < 4 {
                return Err(CodecError::InsufficientData {
                    needed: 4,
                    available: buf.len(),
                });
            }
            let count = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let total = 4 + count * 2;
            if buf.len() < total {
                return Err(CodecError::InsufficientData {
                    needed: total,
                    available: buf.len(),
                });
            }
            let mut values = Vec::with_capacity(count);
            for i in 0..count {
                let offset = 4 + i * 2;
                let v = u16::from_le_bytes([buf[offset], buf[offset + 1]]);
                values.push(v);
            }
            Ok((values, total))
        }
    }

    #[test]
    fn custom_codec_roundtrip() {
        let codec = LengthPrefixedU16Codec;
        let mut buf = Vec::new();
        let original = vec![100u16, 200, 300, 400];
        codec.encode(&original, &mut buf).unwrap();
        let (decoded, consumed) = codec.decode(&buf).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(consumed, 4 + 4 * 2); // 4-byte count + 4 * 2-byte values
    }

    #[test]
    fn custom_codec_error_handling() {
        let codec = LengthPrefixedU16Codec;
        // Says 10 items but only has space for 2
        let mut buf = Vec::new();
        buf.extend_from_slice(&10u32.to_le_bytes());
        buf.extend_from_slice(&[1, 0, 2, 0]); // only 2 items
        let err = codec.decode(&buf).unwrap_err();
        assert_eq!(
            err,
            CodecError::InsufficientData {
                needed: 4 + 10 * 2, // 24
                available: 8
            }
        );
    }

    // --- Codec trait object tests ---

    #[test]
    fn codec_is_object_safe() {
        // Verify Codec can be used as a trait object
        let codec: Box<dyn Codec<u32>> = Box::new(FixedSizeCodec::<u32>::new());
        let mut buf = Vec::new();
        codec.encode(&99u32, &mut buf).unwrap();
        let (val, _) = codec.decode(&buf).unwrap();
        assert_eq!(val, 99u32);
    }

    #[test]
    fn codec_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FixedSizeCodec<u32>>();
        assert_send_sync::<StringCodec>();
        assert_send_sync::<RawBytesCodec>();
    }

    // --- Multiple values in stream ---

    #[test]
    fn decode_stream_of_values() {
        let codec = StringCodec;
        let mut buf = Vec::new();
        let strings = vec!["alpha", "beta", "gamma"];
        for s in &strings {
            codec.encode(&s.to_string(), &mut buf).unwrap();
        }

        let mut offset = 0;
        let mut decoded = Vec::new();
        while offset < buf.len() {
            let (val, consumed) = codec.decode(&buf[offset..]).unwrap();
            decoded.push(val);
            offset += consumed;
        }
        assert_eq!(
            decoded,
            strings.iter().map(|s| s.to_string()).collect::<Vec<_>>()
        );
    }

    // --- MAX_MESSAGE_SIZE guard tests ---

    #[test]
    fn decode_rejects_oversized_length_prefix() {
        // Craft a buffer with a length prefix exceeding MAX_MESSAGE_SIZE
        let huge_len = (MAX_MESSAGE_SIZE + 1) as u32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&huge_len.to_le_bytes());
        buf.extend_from_slice(&[0u8; 100]); // some payload (not enough, but error fires first)

        let codec = StringCodec;
        let err = codec.decode(&buf).unwrap_err();
        match err {
            CodecError::InvalidData(msg) => assert!(msg.contains("exceeds maximum")),
            other => panic!("expected InvalidData, got {other:?}"),
        }

        let raw_codec = RawBytesCodec;
        let err = raw_codec.decode(&buf).unwrap_err();
        match err {
            CodecError::InvalidData(msg) => assert!(msg.contains("exceeds maximum")),
            other => panic!("expected InvalidData, got {other:?}"),
        }
    }
}
