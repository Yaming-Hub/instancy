# Serialization

When data crosses worker or node boundaries, instancy serializes it through the `Codec` abstraction. This page explains the built-in codecs and how to make your own types exchange-safe.

[Back to the guide index](./README.md)

When data crosses worker or node boundaries via exchange operators, it must be serialized. instancy uses a `Codec` trait for this:

```rust
use instancy::communication::codec::{Codec, CodecError};

pub trait Codec<T>: Send + Sync {
    fn encode(&self, value: &T, buf: &mut Vec<u8>) -> Result<(), CodecError>;
    fn decode(&self, buf: &[u8]) -> Result<(T, usize), CodecError>;
}
```

### Built-in Codecs

instancy provides codecs for common types:
- Primitive integers (`u8`, `u16`, `u32`, `u64`, `i8`, `i16`, `i32`, `i64`)
- `String` and `Vec<u8>`
- Tuples `(A, B)` where both components have codecs
- `Product<A, B>` timestamps (for iterate + exchange)

### Implementing ExchangeData

To use your own types with exchange operators, implement `ExchangeData`:

```rust
use instancy::communication::codec::{Codec, CodecError, ExchangeData};

#[derive(Clone, Debug, PartialEq)]
struct MyRecord {
    id: u64,
    name: String,
}

struct MyRecordCodec;

impl Codec<MyRecord> for MyRecordCodec {
    fn encode(&self, value: &MyRecord, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        // Encode id as 8 bytes
        buf.extend_from_slice(&value.id.to_le_bytes());
        // Encode name length + bytes
        let name_bytes = value.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u64).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        Ok(())
    }

    fn decode(&self, buf: &[u8]) -> Result<(MyRecord, usize), CodecError> {
        if buf.len() < 16 {
            return Err(CodecError::InsufficientData);
        }
        let id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let name_len = u64::from_le_bytes(buf[8..16].try_into().unwrap()) as usize;
        if buf.len() < 16 + name_len {
            return Err(CodecError::InsufficientData);
        }
        let name = String::from_utf8(buf[16..16 + name_len].to_vec())
            .map_err(|e| CodecError::Custom(e.to_string()))?;
        Ok((MyRecord { id, name }, 16 + name_len))
    }
}

impl ExchangeData for MyRecord {
    type CodecType = MyRecordCodec;
    fn codec() -> Self::CodecType {
        MyRecordCodec
    }
}
```

With this implementation, `MyRecord` can be used with `exchange` and `exchange_by_hash` in multi-worker and cluster mode.

### Using Bincode

If you prefer automatic serialization, enable the `bincode-codec` feature:

```toml
instancy = { git = "https://github.com/Yaming-Hub/instancy.git", features = ["bincode-codec"] }
```

Then use `BincodeCodec` for any type that implements `serde::Serialize + serde::Deserialize`:

```rust
use instancy::communication::codec::BincodeCodec;

impl ExchangeData for MyRecord {
    type CodecType = BincodeCodec<Self>;
    fn codec() -> Self::CodecType {
        BincodeCodec::new()
    }
}
```

## Related Examples

- [`exchange.rs`](../../instancy/examples/exchange.rs)
- [`cluster_exchange.rs`](../../instancy/examples/cluster_exchange.rs)
- [`cluster_shared_transport.rs`](../../instancy/examples/cluster_shared_transport.rs)

## Next Steps

- Next: [Observability](./observability.md)
- See also: [Distributed Execution](./distributed.md), [API Reference](../reference/api.md)
