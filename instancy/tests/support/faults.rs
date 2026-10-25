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
use instancy::communication::shared_transport::{DynConnectionFactory, SharedPeerManager};
use instancy::communication::transport_session::PeerConnection;
use instancy::communication::{PROGRESS_CHANNEL_BASE, RESERVED_CHANNEL_BASE};

use crate::support::{Managers, PreEstablished, shared_config};

const HEADER: usize = 28;

/// What a frame carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameKind {
    Control,
    Progress,
    Data,
    /// Library-internal frames (reserved namespace, e.g. shared RTT probes):
    /// relayed untouched, never counted, held or corrupted.
    Internal,
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
    /// Withhold frames of a kind from the n-th on (1-based); others pass.
    stash_from: Mutex<Option<(FrameKind, usize)>>,
    stashed: AtomicUsize,
    /// Withhold progress batches; end-of-stream and acknowledgement markers
    /// still pass.
    stash_progress_batches: AtomicBool,
    /// Park frames of this kind, in order, until released; others pass.
    hold_kind: Mutex<Option<FrameKind>>,
    parked: Mutex<VecDeque<([u8; HEADER], Vec<u8>)>>,
    release: Notify,
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

    /// Withhold the `n`-th frame of `kind` (1-based) and every later one,
    /// while frames of other kinds keep flowing: frames of one channel stay
    /// in order, as if the sender had queued the others first (for example,
    /// progress overtaking data held back by backpressure). Withheld frames
    /// are never delivered. Dedicated framing only (shared managers number
    /// their frames, and a gap blocks the receiver).
    pub fn stash_from(&self, kind: FrameKind, n: usize) {
        *self.stash_from.lock().unwrap() = Some((kind, n));
    }

    /// Withhold every progress batch (capability and in-flight changes) while
    /// the progress channels' end-of-stream and acknowledgement markers still
    /// pass: the receiver sees the sender's goodbye but never learns that the
    /// sender released anything, so its frontier cannot complete. Withheld
    /// frames count in [`stashed`](Self::stashed). Dedicated framing only.
    pub fn stash_progress_batches(&self) {
        self.stash_progress_batches.store(true, Ordering::SeqCst);
    }

    /// Delay every frame of `kind` from now on, in order, while frames of
    /// other kinds keep flowing (for example, a peer's progress lane paused
    /// while its data and control pass, or its data held back while its
    /// progress and control overtake it). Nothing is dropped:
    /// [`release_kind`](Self::release_kind) delivers the parked frames ahead
    /// of later ones; the end of the connection waits for the release, so the
    /// parked frames still arrive before it. Dedicated framing
    /// only (shared managers number their frames, and a gap blocks the
    /// receiver).
    pub fn hold_kind(&self, kind: FrameKind) {
        *self.hold_kind.lock().unwrap() = Some(kind);
    }

    /// Stop [`hold_kind`](Self::hold_kind) and deliver the parked frames.
    pub fn release_kind(&self) {
        *self.hold_kind.lock().unwrap() = None;
        self.release.notify_waiters();
    }

    /// Frames parked by [`hold_kind`](Self::hold_kind) and not yet delivered.
    pub fn parked(&self) -> usize {
        self.parked.lock().unwrap().len()
    }

    /// Whether a data channel's close sentinel is parked: its sender has sent
    /// everything on that channel (dedicated framing).
    pub fn parked_close_sentinel(&self) -> bool {
        self.parked
            .lock()
            .unwrap()
            .iter()
            .any(|(_, payload)| payload.as_slice() == [0xFF])
    }

    /// Whether parked frames may be delivered now.
    fn releasable(&self) -> bool {
        self.hold_kind.lock().unwrap().is_none() && !self.parked.lock().unwrap().is_empty()
    }

    /// Frames withheld by [`stash_from`](Self::stash_from) and
    /// [`stash_progress_batches`](Self::stash_progress_batches).
    pub fn stashed(&self) -> usize {
        self.stashed.load(Ordering::SeqCst)
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
            FrameKind::Internal => 0,
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

    /// Whether a progress payload is an end-of-stream or acknowledgement
    /// marker (count `u32::MAX` or `u32::MAX - 1`) rather than a batch.
    fn is_progress_marker(&self, payload: &[u8]) -> bool {
        let start = if self.seq_prefix { 8 } else { 0 };
        payload
            .get(start..start + 4)
            .is_some_and(|count| u32::from_le_bytes(count.try_into().unwrap()) >= u32::MAX - 1)
    }

    fn kind(&self, channel_id: u64) -> FrameKind {
        if channel_id == 0 {
            FrameKind::Control
        } else if self.progress.contains(&channel_id) {
            FrameKind::Progress
        } else if channel_id >= RESERVED_CHANNEL_BASE {
            FrameKind::Internal
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
            FrameKind::Internal => {}
        }
    }
}

/// What the relay does next.
enum Next {
    Frame([u8; HEADER], Vec<u8>),
    /// Parked frames became deliverable.
    Release,
    Eof,
}

/// Reads whole frames, never more bytes than the current frame needs (so a
/// paused relay leaves the sender's backpressure as before), and returns
/// early when parked frames become deliverable. Cancel-safe: bytes read stay
/// in `buf`.
struct FrameReader {
    buf: Vec<u8>,
}

impl FrameReader {
    async fn next(&mut self, from: &mut ReadHalf<DuplexStream>, ctl: &DirCtl) -> Next {
        loop {
            let need = if self.buf.len() < HEADER {
                HEADER
            } else {
                HEADER + u32::from_le_bytes(self.buf[24..28].try_into().unwrap()) as usize
            };
            if self.buf.len() >= HEADER && self.buf.len() == need {
                let frame = std::mem::take(&mut self.buf);
                let header: [u8; HEADER] = frame[..HEADER].try_into().unwrap();
                return Next::Frame(header, frame[HEADER..].to_vec());
            }
            let released = ctl.release.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if ctl.releasable() {
                return Next::Release;
            }
            let mut tmp = vec![0u8; need - self.buf.len()];
            tokio::select! {
                read = from.read(&mut tmp) => match read {
                    Ok(0) | Err(_) => return Next::Eof,
                    Ok(n) => self.buf.extend_from_slice(&tmp[..n]),
                },
                _ = &mut released => {}
            }
        }
    }
}

/// Deliver the parked frames, in order. Returns false if the link broke.
async fn deliver_parked(to: &mut WriteHalf<DuplexStream>, ctl: &DirCtl) -> bool {
    loop {
        let Some((header, payload)) = ctl.parked.lock().unwrap().pop_front() else {
            return true;
        };
        ctl.wait_open().await;
        if to.write_all(&header).await.is_err() || to.write_all(&payload).await.is_err() {
            return false;
        }
    }
}

async fn frame_relay(
    mut from: ReadHalf<DuplexStream>,
    mut to: WriteHalf<DuplexStream>,
    ctl: Arc<DirCtl>,
    classifier: Classifier,
) {
    let mut reader = FrameReader { buf: Vec::new() };
    loop {
        let (header, mut payload) = match reader.next(&mut from, &ctl).await {
            Next::Frame(header, payload) => (header, payload),
            Next::Release => {
                if !deliver_parked(&mut to, &ctl).await {
                    return;
                }
                continue;
            }
            Next::Eof => {
                // Whatever is parked was sent before the connection ended:
                // it is delivered (once released) before the end.
                loop {
                    let released = ctl.release.notified();
                    tokio::pin!(released);
                    released.as_mut().enable();
                    if ctl.hold_kind.lock().unwrap().is_none() {
                        break;
                    }
                    released.await;
                }
                deliver_parked(&mut to, &ctl).await;
                break;
            }
        };
        let channel_id = u64::from_le_bytes(header[16..24].try_into().unwrap());
        let kind = classifier.kind(channel_id);
        let seen = match kind {
            FrameKind::Control => ctl.control.fetch_add(1, Ordering::SeqCst),
            FrameKind::Progress => ctl.progress.fetch_add(1, Ordering::SeqCst),
            FrameKind::Data => ctl.data.fetch_add(1, Ordering::SeqCst),
            FrameKind::Internal => 0,
        } + 1;
        {
            let mut hold = ctl.hold_at.lock().unwrap();
            if matches!(*hold, Some((k, n)) if k == kind && seen >= n) {
                *hold = None;
                ctl.paused.store(true, Ordering::SeqCst);
                ctl.held.store(true, Ordering::SeqCst);
            }
        }
        let stash_batch = kind == FrameKind::Progress
            && ctl.stash_progress_batches.load(Ordering::SeqCst)
            && !classifier.is_progress_marker(&payload);
        if stash_batch
            || matches!(*ctl.stash_from.lock().unwrap(), Some((k, n)) if k == kind && seen >= n)
        {
            ctl.stashed.fetch_add(1, Ordering::SeqCst);
            continue;
        }
        if *ctl.hold_kind.lock().unwrap() == Some(kind) {
            ctl.parked.lock().unwrap().push_back((header, payload));
            continue;
        }
        // Parked frames that became deliverable go first.
        if ctl.hold_kind.lock().unwrap().is_none() && !deliver_parked(&mut to, &ctl).await {
            break;
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
