//! `ProgressExchange::broadcast_local_changes` retries after backpressure.
//!
//! Progress deltas are not idempotent. When one peer's buffer is full, peers
//! that already accepted the update must not receive it again on the
//! documented retry with the same changes.

#![cfg(feature = "transport")]

use std::sync::Arc;

use instancy::DataflowId;
use instancy::communication::codec::{Codec, CodecError, FixedSizeCodec};
use instancy::communication::progress_exchange::{PeerProgressSender, ProgressExchange};
use instancy::error::Error;

struct U64Codec;

impl Codec<u64> for U64Codec {
    fn encode(&self, value: &u64, buf: &mut Vec<u8>) -> Result<(), CodecError> {
        buf.extend_from_slice(&value.to_le_bytes());
        Ok(())
    }

    fn decode(&self, buf: &[u8]) -> Result<(u64, usize), CodecError> {
        if buf.len() < 8 {
            return Err(CodecError::InsufficientData {
                needed: 8,
                available: buf.len(),
            });
        }
        Ok((u64::from_le_bytes(buf[..8].try_into().unwrap()), 8))
    }
}

#[test]
fn retry_after_backpressure_does_not_duplicate_delta_to_accepting_peer() {
    let (p1, r1) = PeerProgressSender::new("node-1", 16);
    let (p2, r2) = PeerProgressSender::new("node-2", 1);
    let ex = ProgressExchange::new(
        DataflowId::new(),
        "node-0",
        vec![p1, p2],
        Arc::new(U64Codec),
    );
    // Fill node-2's buffer.
    ex.broadcast_local_changes(&[(0, 0u64, 1)]).unwrap();
    // node-1 accepts, node-2 is full.
    let delta = [(1usize, 7u64, -1i64)];
    assert!(ex.broadcast_local_changes(&delta).is_err());
    // Retry as documented once node-2 drains.
    r2.recv().unwrap();
    ex.broadcast_local_changes(&delta).unwrap();

    let mut frames = 0;
    while r1.try_recv().is_some() {
        frames += 1;
    }
    assert_eq!(
        frames, 2,
        "node-1 must get the first update and the delta once"
    );
    assert_eq!(r2.drain().len(), 1, "node-2 must get the delta once");
}

#[test]
fn documented_retry_delivers_each_peer_the_delta_once() {
    let (p1, rx1) = PeerProgressSender::new("peer-1", 16);
    let (p2, rx2) = PeerProgressSender::new("peer-2", 1);
    let ex = ProgressExchange::<u64, _>::new(
        DataflowId::new(),
        "local",
        vec![p1, p2],
        Arc::new(FixedSizeCodec::<u64>::new()),
    );
    // Fill peer-2's buffer.
    ex.broadcast_local_changes(&[(0, 0u64, 1)]).unwrap();
    rx1.drain();
    let changes = [(3usize, 5u64, -1i64)];
    assert!(matches!(
        ex.broadcast_local_changes(&changes),
        Err(Error::Backpressure)
    ));
    // Drain and retry with the same changes.
    rx2.drain();
    ex.broadcast_local_changes(&changes).unwrap();

    for (peer, rx) in [("peer-1", &rx1), ("peer-2", &rx2)] {
        let received: Vec<_> = rx
            .drain()
            .into_iter()
            .map(|f| ex.decode_remote_progress(&f.payload).unwrap().changes)
            .collect();
        assert_eq!(received, vec![changes.to_vec()], "{peer}");
    }
}
