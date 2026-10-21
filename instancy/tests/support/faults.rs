//! Frame-aware fault injection for multi-node cluster tests.
//!
//! [`FaultMesh`] is a full mesh of in-process links whose relays parse the
//! transport's frame header, so a test can corrupt the next frame of a given
//! kind (data, progress or control), pause a direction (a silent partition:
//! the connection stays open), or cut a link (both ends see EOF). It works
//! for dedicated transports and, with `seq_prefix`, for shared managers
//! (whose payload frames carry an 8-byte sequence number).
//!
//! Frames are assumed to be written without frame CRC (the default for both
//! transport modes).

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use instancy::communication::ClusterSpawnTransport;
use instancy::communication::PROGRESS_CHANNEL_BASE;
use instancy::communication::shared_transport::{DynConnectionFactory, SharedPeerManager};
use instancy::communication::transport_session::PeerConnection;

use crate::support::{Managers, PreEstablished, shared_config};

const HEADER: usize = 28;

/// What a frame carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameKind {
    Control,
    Progress,
    Data,
}

/// Controls for one direction of a link.
#[derive(Default)]
pub struct DirCtl {
    paused: AtomicBool,
    notify: Notify,
    corrupt_next: Mutex<Option<FrameKind>>,
    corrupted: AtomicBool,
    control: AtomicUsize,
    progress: AtomicUsize,
    data: AtomicUsize,
    inject: Mutex<VecDeque<(u64, Vec<u8>)>>,
    /// Payload to inject on a data channel right after its close sentinel
    /// (dedicated framing only).
    after_sentinel: Mutex<Option<Vec<u8>>>,
    injected_after_sentinel: AtomicBool,
    /// Pause in front of the n-th frame of a kind (1-based).
    hold_at: Mutex<Option<(FrameKind, usize)>>,
    held: AtomicBool,
}

impl DirCtl {
    /// Stop forwarding (the sender sees backpressure; the connection stays up).
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Pause forwarding in front of the `n`-th frame of `kind` (1-based), so
    /// that frame and everything after it is held back.
    pub fn hold_from(&self, kind: FrameKind, n: usize) {
        *self.hold_at.lock().unwrap() = Some((kind, n));
    }

    /// Whether [`hold_from`](Self::hold_from) has paused the link.
    pub fn held(&self) -> bool {
        self.held.load(Ordering::SeqCst)
    }

    /// Corrupt the next frame of `kind` so that it fails to decode.
    pub fn corrupt_next(&self, kind: FrameKind) {
        *self.corrupt_next.lock().unwrap() = Some(kind);
    }

    /// Whether a frame has been corrupted.
    pub fn corrupted(&self) -> bool {
        self.corrupted.load(Ordering::SeqCst)
    }

    /// Frames of `kind` forwarded so far.
    pub fn count(&self, kind: FrameKind) -> usize {
        match kind {
            FrameKind::Control => self.control.load(Ordering::SeqCst),
            FrameKind::Progress => self.progress.load(Ordering::SeqCst),
            FrameKind::Data => self.data.load(Ordering::SeqCst),
        }
    }

    /// Forward an extra frame `(channel_id, payload)` after the next frame.
    pub fn inject_after_next(&self, channel_id: u64, payload: Vec<u8>) {
        self.inject.lock().unwrap().push_back((channel_id, payload));
    }

    /// After the next close sentinel on a data channel, forward `payload` on
    /// the same channel (a frame after the end of stream).
    pub fn inject_after_close_sentinel(&self, payload: Vec<u8>) {
        *self.after_sentinel.lock().unwrap() = Some(payload);
    }

    /// Whether [`inject_after_close_sentinel`](Self::inject_after_close_sentinel) fired.
    pub fn injected_after_sentinel(&self) -> bool {
        self.injected_after_sentinel.load(Ordering::SeqCst)
    }

    async fn wait_open(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.paused.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

/// Channel-id classification shared by both directions of every link.
#[derive(Clone)]
pub struct Classifier {
    progress: Range<u64>,
    /// Shared managers prefix payload (non-control) frames with an 8-byte
    /// sequence number.
    seq_prefix: bool,
}

impl Classifier {
    pub fn new(total_workers: usize, seq_prefix: bool) -> Self {
        let n = total_workers as u64;
        Self {
            progress: PROGRESS_CHANNEL_BASE..PROGRESS_CHANNEL_BASE + n * n,
            seq_prefix,
        }
    }

    fn kind(&self, channel_id: u64) -> FrameKind {
        if channel_id == 0 {
            FrameKind::Control
        } else if self.progress.contains(&channel_id) {
            FrameKind::Progress
        } else {
            FrameKind::Data
        }
    }

    /// Damage `payload` so it cannot be decoded.
    fn corrupt(&self, kind: FrameKind, payload: &mut [u8]) {
        let start = if self.seq_prefix && kind != FrameKind::Control {
            8
        } else {
            0
        };
        match kind {
            // Progress and control frames carry a CRC32 trailer: any flipped
            // byte is detected.
            FrameKind::Progress | FrameKind::Control => {
                if let Some(byte) = payload.get_mut(start) {
                    *byte ^= 0xFF;
                }
            }
            // Data frames start with a tag byte; 0x7E is not a valid tag.
            FrameKind::Data => {
                if let Some(byte) = payload.get_mut(start) {
                    *byte = 0x7E;
                }
            }
        }
    }
}

async fn frame_relay(
    mut from: ReadHalf<DuplexStream>,
    mut to: WriteHalf<DuplexStream>,
    ctl: Arc<DirCtl>,
    classifier: Classifier,
) {
    loop {
        let mut header = [0u8; HEADER];
        if from.read_exact(&mut header).await.is_err() {
            break;
        }
        let channel_id = u64::from_le_bytes(header[16..24].try_into().unwrap());
        let len = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;
        let mut payload = vec![0u8; len];
        if from.read_exact(&mut payload).await.is_err() {
            break;
        }
        let kind = classifier.kind(channel_id);
        let seen = match kind {
            FrameKind::Control => ctl.control.fetch_add(1, Ordering::SeqCst),
            FrameKind::Progress => ctl.progress.fetch_add(1, Ordering::SeqCst),
            FrameKind::Data => ctl.data.fetch_add(1, Ordering::SeqCst),
        } + 1;
        {
            let mut hold = ctl.hold_at.lock().unwrap();
            if matches!(*hold, Some((k, n)) if k == kind && seen >= n) {
                *hold = None;
                ctl.paused.store(true, Ordering::SeqCst);
                ctl.held.store(true, Ordering::SeqCst);
            }
        }
        {
            let mut corrupt = ctl.corrupt_next.lock().unwrap();
            if *corrupt == Some(kind) {
                *corrupt = None;
                classifier.corrupt(kind, &mut payload);
                ctl.corrupted.store(true, Ordering::SeqCst);
            }
        }
        ctl.wait_open().await;
        if to.write_all(&header).await.is_err() || to.write_all(&payload).await.is_err() {
            break;
        }
        let mut injected: Vec<_> = ctl.inject.lock().unwrap().drain(..).collect();
        if kind == FrameKind::Data && payload.as_slice() == [0xFF] {
            if let Some(extra) = ctl.after_sentinel.lock().unwrap().take() {
                injected.push((channel_id, extra));
                ctl.injected_after_sentinel.store(true, Ordering::SeqCst);
            }
        }
        for (channel_id, payload) in injected {
            let mut extra = header;
            extra[16..24].copy_from_slice(&channel_id.to_le_bytes());
            extra[24..28].copy_from_slice(&(payload.len() as u32).to_le_bytes());
            if to.write_all(&extra).await.is_err() || to.write_all(&payload).await.is_err() {
                return;
            }
        }
    }
    let _ = to.shutdown().await;
}

/// A full mesh of frame-aware in-process links.
pub struct FaultMesh {
    ends: HashMap<(String, String), DuplexStream>,
    ctls: HashMap<(String, String), Arc<DirCtl>>,
    relays: HashMap<(String, String), Vec<JoinHandle<()>>>,
}

impl FaultMesh {
    /// `total_workers` sizes the progress channel-id range; `seq_prefix` must
    /// be true when the links carry shared-manager traffic.
    pub fn new(nodes: &[&str], total_workers: usize, seq_prefix: bool) -> Self {
        let classifier = Classifier::new(total_workers, seq_prefix);
        let mut mesh = Self {
            ends: HashMap::new(),
            ctls: HashMap::new(),
            relays: HashMap::new(),
        };
        for (i, a) in nodes.iter().enumerate() {
            for b in &nodes[i + 1..] {
                let (a_end, relay_a) = tokio::io::duplex(256 * 1024);
                let (b_end, relay_b) = tokio::io::duplex(256 * 1024);
                let (ra_read, ra_write) = tokio::io::split(relay_a);
                let (rb_read, rb_write) = tokio::io::split(relay_b);
                let a_to_b = Arc::new(DirCtl::default());
                let b_to_a = Arc::new(DirCtl::default());
                let relays = vec![
                    tokio::spawn(frame_relay(
                        ra_read,
                        rb_write,
                        Arc::clone(&a_to_b),
                        classifier.clone(),
                    )),
                    tokio::spawn(frame_relay(
                        rb_read,
                        ra_write,
                        Arc::clone(&b_to_a),
                        classifier.clone(),
                    )),
                ];
                let (a, b) = (a.to_string(), b.to_string());
                mesh.ends.insert((a.clone(), b.clone()), a_end);
                mesh.ends.insert((b.clone(), a.clone()), b_end);
                mesh.ctls.insert((a.clone(), b.clone()), a_to_b);
                mesh.ctls.insert((b.clone(), a.clone()), b_to_a);
                mesh.relays.insert((a, b), relays);
            }
        }
        mesh
    }

    /// Controls for the `from -> to` direction.
    pub fn ctl(&self, from: &str, to: &str) -> Arc<DirCtl> {
        Arc::clone(&self.ctls[&(from.to_string(), to.to_string())])
    }

    /// Kill the link between `a` and `b` (both directions), like a crash.
    pub fn cut(&self, a: &str, b: &str) {
        let key = if self.relays.contains_key(&(a.to_string(), b.to_string())) {
            (a.to_string(), b.to_string())
        } else {
            (b.to_string(), a.to_string())
        };
        for relay in &self.relays[&key] {
            relay.abort();
        }
    }

    /// Cut every link.
    pub fn cut_all(&self) {
        for relays in self.relays.values() {
            for relay in relays {
                relay.abort();
            }
        }
    }

    /// Pause both directions between `a` and `b`: a silent partition.
    pub fn partition(&self, a: &str, b: &str) {
        self.ctl(a, b).pause();
        self.ctl(b, a).pause();
    }

    fn take_ends(&mut self, node: &str) -> Vec<(String, DuplexStream)> {
        let keys: Vec<_> = self
            .ends
            .keys()
            .filter(|(from, _)| from == node)
            .cloned()
            .collect();
        keys.into_iter()
            .map(|key| {
                let stream = self.ends.remove(&key).unwrap();
                (key.1, stream)
            })
            .collect()
    }

    /// Dedicated-mode transport for `node`.
    pub fn dedicated(
        &mut self,
        node: &str,
        capacity: usize,
    ) -> ClusterSpawnTransport<ReadHalf<DuplexStream>, WriteHalf<DuplexStream>> {
        let conns = self
            .take_ends(node)
            .into_iter()
            .map(|(peer, stream)| {
                let (reader, writer) = tokio::io::split(stream);
                PeerConnection {
                    node_id: peer,
                    reader,
                    writer,
                }
            })
            .collect();
        ClusterSpawnTransport::dedicated(conns, capacity)
    }

    /// Shared-mode peer managers for `node` (one pooled connection per peer;
    /// a cut connection cannot be re-established).
    pub fn managers(&mut self, node: &str) -> Managers {
        let handle = tokio::runtime::Handle::current();
        let mut managers = HashMap::new();
        for (peer, stream) in self.take_ends(node) {
            let factory: Arc<dyn DynConnectionFactory> =
                Arc::new(PreEstablished(Mutex::new(VecDeque::from([
                    tokio::io::split(stream),
                ]))));
            managers.insert(
                peer.clone(),
                SharedPeerManager::new(
                    peer,
                    shared_config(1),
                    factory,
                    &handle,
                    tokio::sync::broadcast::channel(16).0,
                )
                .unwrap(),
            );
        }
        Arc::new(managers)
    }
}
