# Pluggable Serialization

This document defines how instancy serializes data across process boundaries without hard-coding a single wire format.

Back to the overview: [Design Overview](./README.md)

## 7. Pluggable Serialization

### 7.1 Codec Trait

```rust
/// Trait for serializing/deserializing data on the wire.
pub trait Codec<T>: Send + Sync + 'static {
    /// Serializes `item` into `buf`. Returns bytes written.
    fn encode(&self, item: &T, buf: &mut BytesMut) -> Result<(), Error>;
    
    /// Deserializes an item from `buf`, advancing the cursor.
    fn decode(&self, buf: &mut Bytes) -> Result<T, Error>;
}
```

### 7.2 Default: Bincode

```rust
pub struct BincodeCodec<T> {
    _phantom: PhantomData<T>,
    config: bincode::config::Configuration,
}

impl<T: Serialize + DeserializeOwned> Codec<T> for BincodeCodec<T> { ... }
```

### 7.3 Data Bounds

```rust
/// Data that can be exchanged across workers within a process.
pub trait Data: Clone + Send + Sync + 'static {}

/// Data that can be exchanged across processes (requires serialization).
pub trait ExchangeData: Data {
    /// The codec type used for serialization.
    type Codec: Codec<Self>;
    
    /// Returns the codec to use for this data type.
    fn codec() -> Self::Codec;
}
```

**Alternative**: supply the codec at channel allocation time rather than tying it to the data type, for maximum flexibility:

```rust
fn exchange_with_codec<D, C>(
    &self,
    route: impl Fn(&D) -> u64 + Send + Sync + 'static,
    codec: C,
) -> StreamEdge<S, Vec<D>>
where
    C: Codec<Vec<D>>;
```
