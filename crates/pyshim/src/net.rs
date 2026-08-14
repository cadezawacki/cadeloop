//! L2 TCP transports, listeners, and readiness watches — the M1 layer.
//!
//! Runs identically over IOCP (Windows) and epoll (Linux dev) through the
//! unified completion API. Performance techniques:
//!
//! * protocol callbacks cached as bound methods at connection setup
//!   (R-054) and invoked without attribute lookups;
//! * recv pipelining: for plain `Protocol`s the payload `bytes` object is
//!   materialized during completion translation and the SAME pooled slot is
//!   re-posted immediately, so the kernel refills while Python processes;
//! * corked writes (R-035): `write()` queues; a gather `WSASend/writev` of
//!   up to 16 slices is flushed at tick end, immediately at >= 64 KiB, or
//!   on send completion — `bytes` payloads are retained zero-copy (R-074),
//!   other buffer exporters are copied at call time;
//! * water-mark backpressure (R-122): pause_writing above `high_water`
//!   (default 64 KiB), resume_writing below `low_water` (16 KiB).
//!
//! Re-entrancy discipline: NOTHING under the state cell runs user Python or
//! drops Python refs. Completion translation produces [`NetEvent`]s (any
//! `bytes` payloads are pre-built — allocating non-GC-tracked objects
//! cannot trigger the cyclic GC) which are dispatched after the borrow
//! ends; torn-down entries land in graveyards dropped outside; transport
//! methods called from protocol callbacks re-enter the cell one call at a
//! time.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::time::{Duration, Instant};

use cadeloop_core::backend::{is_cancelled_error, Completion, IoBackend, IoSlice, RawSocket};
use cadeloop_core::buffers::{BufferPool, SizeClass, SlotId};
use cadeloop_core::http::{Limits, ParseError};
use cadeloop_core::netsys;
use cadeloop_core::opslab::OpId;
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use crate::coreloop::CoreLoop;
use crate::http::HttpConn;

/// R-035: cork flush threshold.
const CORK_FLUSH_BYTES: usize = 64 * 1024;
/// Recv slot class: 64 KiB reads.
const RECV_CLASS: SizeClass = SizeClass::S64K;

pub(crate) fn os_err(py: Python<'_>, code: u32) -> Py<PyAny> {
    let err: PyErr = io::Error::from_raw_os_error(code as i32).into();
    err.into_value(py).into_any()
}

// --------------------------------------------------------------------- //
// state                                                                 //
// --------------------------------------------------------------------- //

pub(crate) enum OpTarget {
    Recv {
        tid: u64,
        slot: SlotId,
    },
    Send(u64),
    Connect {
        fut: Py<PyAny>,
        sock: RawSocket,
    },
    Accept(u64),
    /// R-058 datagram endpoint ops (did keys `NetState.datagrams`).
    DgramRecv(u64),
    DgramSend(u64),
    /// R-051 Windows named-pipe ops: `fut` resolves with the bytes read
    /// (sliced from `buf`, which the backend wrote into via a raw
    /// pointer — kept alive here until the completion is reaped).
    PipeRead {
        fut: Py<PyAny>,
        buf: Vec<u8>,
    },
    /// `buf` is never read back — its only job is keeping the write data
    /// alive (pinned) until WriteFile's completion (same discipline as
    /// `WriteBuf::Bytes`'s `_keep`).
    PipeWrite {
        fut: Py<PyAny>,
        _buf: Vec<u8>,
    },
}

/// Resources a cancelled kernel operation still owns.
///
/// `CancelIoEx` only *requests* cancellation: IOCP may keep reading a send
/// buffer or writing a recv buffer until that op's (ABORTED) completion is
/// dequeued. Teardown therefore hands the op's buffers here instead of
/// freeing them, and the reap in `dispatch_completions` releases them once
/// the kernel is provably finished (R-037/R-073).
pub(crate) enum ReapGuard {
    /// A pool slot the kernel may still write into.
    Slot(SlotId),
    /// Write buffers the kernel may still read from. Dropped via
    /// `graveyard_bufs`, never in-cell: `WriteBuf::Bytes` owns a `Py`.
    Writes(Vec<WriteBuf>),
}

pub(crate) enum WriteBuf {
    /// Zero-copy retained `bytes` (R-074). `ptr` stays valid while `keep`
    /// holds the reference (bytes are immutable and pinned by refcount).
    /// `_keep` is never read — its only job is holding the refcount
    /// that pins `ptr` (dropped via the graveyard).
    Bytes { _keep: Py<PyAny>, ptr: *const u8, len: usize, off: usize },
    /// Copied at `write()` time (bytearray & other exporters, R-074).
    Owned { data: Vec<u8>, off: usize },
}

impl WriteBuf {
    fn remaining(&self) -> usize {
        match self {
            WriteBuf::Bytes { len, off, .. } => len - off,
            WriteBuf::Owned { data, off } => data.len() - off,
        }
    }

    fn slice(&self) -> IoSlice {
        match self {
            WriteBuf::Bytes { ptr, len, off, .. } => {
                IoSlice { ptr: unsafe { ptr.add(*off) }, len: (len - off) as u32 }
            }
            WriteBuf::Owned { data, off } => {
                IoSlice { ptr: unsafe { data.as_ptr().add(*off) }, len: (data.len() - off) as u32 }
            }
        }
    }

    fn advance(&mut self, n: usize) {
        match self {
            WriteBuf::Bytes { off, .. } | WriteBuf::Owned { off, .. } => *off += n,
        }
    }
}

pub(crate) struct ProtoRefs {
    pub protocol: Py<PyAny>,
    pub data_received: Option<Py<PyAny>>,
    pub get_buffer: Option<Py<PyAny>>,
    pub buffer_updated: Option<Py<PyAny>>,
    pub eof_received: Py<PyAny>,
    pub connection_lost: Py<PyAny>,
    pub pause_writing: Py<PyAny>,
    pub resume_writing: Py<PyAny>,
}

/// What consumes inbound bytes on a transport: a Python protocol object
/// (asyncio surface) or the native HTTP/ASGI engine (M2, R-080).
pub(crate) enum ProtoKind {
    Py(ProtoRefs),
    Http(Box<HttpConn>),
}

pub(crate) struct TransportEntry {
    pub socket: RawSocket,
    pub proto: ProtoKind,
    /// Python Transport facade; `None` for native HTTP connections (no
    /// user-visible object ever references them).
    pub pyobj: Option<Py<Transport>>,
    recv_slot: Option<SlotId>,
    recv_op: Option<OpId>,
    send_op: Option<OpId>,
    wq: VecDeque<WriteBuf>,
    queued_bytes: usize,
    high_water: usize,
    low_water: usize,
    reading_paused: bool,
    proto_paused: bool,
    closing: bool,
    conn_lost: bool,
    eof_wanted: bool,
    eof_sent: bool,
    flush_scheduled: bool,
    pub peername: Option<Py<PyAny>>,
    pub sockname: Option<Py<PyAny>>,
    /// Bytes that completed while `reading_paused` was set (R-034).
    /// Held rather than delivered: a protocol pauses to bound its own
    /// memory, and handing it another read defeats exactly that.
    /// Delivered by `resume_reading`.
    paused_recv: Option<Vec<u8>>,
    /// `get_extra_info("socket")`, built on first ask and kept. Moved to
    /// `graveyard_sockets` at teardown to be closed, not merely dropped
    /// -- see the note there.
    pub extra_sock: Option<Py<PyAny>>,
}

/// What an accepted connection turns into.
/// R-080 connection-timeout tuning, fixed per listener (0 = disabled).
#[derive(Clone, Copy)]
pub(crate) struct HttpTuning {
    pub head_timeout_ns: u64,
    pub idle_timeout_ns: u64,
}

pub(crate) enum ListenerKind {
    /// asyncio create_server: `protocol_factory()` per accept.
    Factory(Py<PyAny>),
    /// Native HTTP engine (M2): every accept becomes an [`HttpConn`].
    /// `pyloop` is the facade loop (receive() waiter futures need it);
    /// `state` is the lifespan state dict, shallow-copied per scope.
    Http {
        app: Py<PyAny>,
        pyloop: Py<PyAny>,
        state: Py<PyAny>,
        limits: Limits,
        eager: bool,
        tuning: HttpTuning,
        /// R-059: ssl.SSLContext for native TLS termination (None = plaintext).
        tls: Option<Py<PyAny>>,
    },
}

pub(crate) struct ListenerEntry {
    pub socket: RawSocket,
    pub kind: ListenerKind,
    accept_ops: Vec<OpId>,
    target: usize,
    closing: bool,
    /// The pool ran dry and `post_accept` refused (transiently, e.g. a
    /// descriptor limit). Nothing is outstanding, so no completion can
    /// wake this listener again -- the next tick retries instead.
    starved: bool,
}

/// R-058 datagram endpoint state (cached protocol callbacks, one
/// outstanding recv, serialized sends).
pub(crate) struct DatagramEntry {
    pub socket: RawSocket,
    pub datagram_received: Py<PyAny>,
    pub error_received: Py<PyAny>,
    pub connection_lost: Py<PyAny>,
    pub recv_op: Option<OpId>,
    pub recv_slot: Option<SlotId>,
    pub send_op: Option<OpId>,
    /// Sends beyond the in-flight one (the backend allows one parked
    /// write-side op per fd).
    pub send_queue: VecDeque<(Vec<u8>, Option<std::net::SocketAddr>)>,
    /// Bytes sitting in `send_queue`. Reported through the transport's
    /// `get_write_buffer_size()`, which used to always answer zero.
    pub queued_bytes: usize,
    /// The recv could not be reposted (transient). Nothing is outstanding
    /// to deliver the completion that would retry, so the tick does.
    pub recv_starved: bool,
    pub closing: bool,
    pub conn_lost: bool,
}

/// Ceiling on a datagram endpoint's queued output (R-058). UDP is lossy
/// by contract, so the deterministic overflow policy is to drop the
/// datagram and report ENOBUFS through `error_received` -- unlike a
/// stream, there is nothing to be gained by growing without bound.
pub(crate) const DGRAM_SEND_QUEUE_MAX: usize = 1 << 20;

/// Reported to `error_received` when a datagram is dropped for queue
/// overflow. WSAENOBUFS on Windows, ENOBUFS on Linux -- the same errno an
/// oversubscribed kernel socket buffer raises, which is what callers
/// already expect to see here.
#[cfg(windows)]
const ENOBUFS: u32 = 10055; // WSAENOBUFS
#[cfg(not(windows))]
const ENOBUFS: u32 = 105; // ENOBUFS on Linux

#[derive(Default)]
pub(crate) struct NetState {
    pub ops: HashMap<OpId, OpTarget>,
    pub transports: HashMap<u64, TransportEntry>,
    pub listeners: HashMap<u64, ListenerEntry>,
    next_id: u64,
    pub buffers: BufferPool,
    pub readers: HashMap<RawSocket, Py<PyAny>>,
    pub writers: HashMap<RawSocket, Py<PyAny>>,
    pub flush_list: Vec<u64>,
    /// Events awaiting phase-2 dispatch (outside the state cell).
    pub events: Vec<NetEvent>,
    /// Watch-callback handles to enqueue on the reactor ready queue.
    pub ready_scratch: Vec<Py<PyAny>>,
    /// Python refs & buffers to drop outside the cell (see gil_boundary).
    /// Buffers owned by cancelled-but-unreaped ops, keyed by op.
    pub reap_guards: HashMap<OpId, ReapGuard>,
    /// Close codes of recently torn-down WebSocket connections, so a
    /// receive() arriving after teardown still reports
    /// `websocket.disconnect` with the peer's code instead of
    /// `http.disconnect`. Bounded: a late receive lands within a tick or
    /// two of the teardown, never thousands of connections later.
    pub recent_ws_closes: VecDeque<(u64, u16)>,
    /// Pipe ops cancelled during loop close. Their buffers must outlive
    /// the cancellation request; they are released when the state is
    /// dropped, after the backend (and with it the handles) has gone.
    pub closed_pipe_ops: Vec<OpTarget>,
    pub graveyard_entries: Vec<TransportEntry>,
    pub graveyard_bufs: Vec<WriteBuf>,
    pub graveyard_py: Vec<Py<PyAny>>,
    /// Sockets handed out by `get_extra_info("socket")`, awaiting an
    /// explicit `close()` outside the state cell (see drain_graveyards).
    pub graveyard_sockets: Vec<Py<PyAny>>,
    pub graveyard_protos: Vec<ProtoKind>,
    /// R-058 datagram endpoints (separate from stream transports: no
    /// write-queue/backpressure machinery, per-packet semantics).
    pub datagrams: HashMap<u64, DatagramEntry>,
    /// R-140 access-log sink (a Python callable) — None disables logging
    /// with a single branch on the request-completion path.
    pub access_sink: Option<Py<PyAny>>,
    pub stats_bytes_rx: u64,
    pub stats_bytes_tx: u64,
    pub stats_conns_accepted: u64,
    /// Times an accept pool ran dry on a transient post failure (R-032).
    pub stats_accept_starved: u64,
    /// Sends posted to the kernel (R-035). Read against `bytes_sent`, this
    /// is the direct measure of what corking buys: the same bytes over
    /// fewer syscalls. It is also how the immediate-flush latency mode is
    /// tested, since its effect is a send count, not a wall-clock number
    /// this class of hardware can resolve.
    pub stats_sends_posted: u64,
    /// Times a connection stopped reading because its pipeline budget was
    /// spent (R-085). Non-zero means backpressure is doing its job.
    pub stats_pipeline_pauses: u64,
    /// Cheap gate for `retry_starved_listeners` on the tick path.
    pub any_starved_listener: bool,
    /// R-060 latency mode: put HTTP response bytes on the wire the moment
    /// they are wire-ready instead of corking them until the tick's flush
    /// phase. Off by default -- corking is the throughput choice and the
    /// right default -- but it is what a latency-SLA deployment wants,
    /// because a response that was ready first can otherwise wait behind
    /// however many *other* connections' app dispatch the same tick
    /// batched in front of it. Deliberately scoped to the HTTP engine:
    /// a Python protocol issuing many small `write()`s is exactly the
    /// case corking exists for.
    pub flush_immediately: bool,
    /// HTTP `Date:` header cache (R-084): rebuilt when the unix second ticks.
    pub http_date_secs: u64,
    pub http_date_line: Vec<u8>,
    /// unix wall time = monotonic seconds + this offset (captured once).
    unix_offset_secs: i64,
}

// SAFETY: thread-affine by the gil_boundary protocol — only the owner
// thread touches NetState (raw pointers into retained buffers / pool slabs
// never cross threads).
unsafe impl Send for NetState {}

impl NetState {
    pub(crate) fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// Unix wall seconds derived from the reactor's cached monotonic clock.
    /// The wall/monotonic offset is captured once — Date headers tolerate
    /// the (sub-second, non-cumulative) drift.
    pub(crate) fn unix_now_secs(&mut self, now_ns: u64) -> u64 {
        let mono = (now_ns / 1_000_000_000) as i64;
        if self.unix_offset_secs == 0 {
            let unix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0) as i64;
            self.unix_offset_secs = unix - mono;
        }
        (mono + self.unix_offset_secs).max(0) as u64
    }

    /// The native HTTP connection behind `tid`, if that transport is one.
    /// NOTE: borrows the whole NetState — take what you need out of the
    /// returned `&mut HttpConn` before touching other NetState fields.
    /// How many live connections are native HTTP ones (as opposed to the
    /// Python-protocol transports an application opens for itself).
    pub(crate) fn http_conn_count(&self) -> usize {
        self.transports.values().filter(|e| matches!(e.proto, ProtoKind::Http(_))).count()
    }

    pub(crate) fn http_conn_mut(&mut self, tid: u64) -> Option<&mut HttpConn> {
        match self.transports.get_mut(&tid) {
            Some(TransportEntry { proto: ProtoKind::Http(conn), .. }) => Some(conn),
            _ => None,
        }
    }

    /// (peername, sockname) for ASGI scope construction.
    pub(crate) fn peer_local(&self, py: Python<'_>, tid: u64) -> (Option<Py<PyAny>>, Option<Py<PyAny>>) {
        match self.transports.get(&tid) {
            Some(e) => {
                (e.peername.as_ref().map(|p| p.clone_ref(py)), e.sockname.as_ref().map(|p| p.clone_ref(py)))
            }
            None => (None, None),
        }
    }
}

/// Phase-2 events: dispatched with the GIL, outside the state cell.
pub(crate) enum NetEvent {
    /// Plain-protocol data: payload prebuilt, slot already re-posted.
    Data {
        /// Needed so a failing callback can close the connection it
        /// arrived on -- see the dispatch site.
        tid: u64,
        data_received: Py<PyAny>,
        payload: Py<PyAny>,
    },
    /// R-058: one received datagram (payload prebuilt, recv re-posted).
    DgramData {
        datagram_received: Py<PyAny>,
        payload: Py<PyAny>,
        addr: Option<std::net::SocketAddr>,
    },
    /// R-058: per-packet error surfaced to protocol.error_received.
    DgramError {
        error_received: Py<PyAny>,
        err: u32,
    },
    /// R-058: datagram endpoint torn down.
    DgramLost {
        connection_lost: Py<PyAny>,
        err: Option<u32>,
    },
    /// R-087: a WS receive() waiter has a queued event to consume.
    /// R-084 ASGI write backpressure: the native `send()` returned a
    /// pending awaitable because the write queue was over its high-water
    /// mark; resolve it now that it has drained (or the connection died,
    /// which must also release the app rather than hang it forever).
    HttpDrained {
        fut: Py<PyAny>,
    },
    WsWake {
        tid: u64,
        fut: Py<PyAny>,
    },
    /// R-059: inbound ciphertext for a TLS connection (record processing
    /// needs Python's _ssl, so it runs in phase 2).
    TlsData {
        tid: u64,
        data: Vec<u8>,
    },
    /// R-059: staged plaintext awaiting encryption (see http_enqueue).
    TlsFlush {
        tid: u64,
    },
    /// BufferedProtocol data: copy out of the retained slot in phase 2
    /// (get_buffer may run arbitrary Python, so it cannot run in-cell).
    /// Buffered-protocol delivery from an owned copy: the held bytes a
    /// paused transport accumulated, which have no slot of their own.
    BufDataOwned {
        tid: u64,
        get_buffer: Py<PyAny>,
        buffer_updated: Py<PyAny>,
        data: Vec<u8>,
    },
    BufData {
        tid: u64,
        get_buffer: Py<PyAny>,
        buffer_updated: Py<PyAny>,
        slot: SlotId,
        len: usize,
    },
    Eof {
        eof_received: Py<PyAny>,
        transport: Py<Transport>,
    },
    ConnLost {
        connection_lost: Py<PyAny>,
        err: Option<u32>,
    },
    ResumeWriting {
        resume_writing: Py<PyAny>,
    },
    Accepted {
        lid: u64,
        sock: RawSocket,
    },
    AcceptError {
        err: u32,
    },
    ConnectDone {
        fut: Py<PyAny>,
        sock: RawSocket,
        err: u32,
    },
    /// Native HTTP: parsed request(s) queued — run the request pump.
    HttpPump {
        tid: u64,
    },
    /// Native HTTP: resolve a pending `receive()` waiter with
    /// `http.disconnect` (or `websocket.disconnect` for WS sessions).
    HttpDisconnect {
        fut: Py<PyAny>,
        ws: bool,
    },
    /// R-051: a pipe ReadFile completed — `buf[..bytes]` is the data (0
    /// bytes = EOF, matching stdlib's proactor `recv()` convention).
    PipeReadDone {
        fut: Py<PyAny>,
        buf: Vec<u8>,
        bytes: u32,
        err: u32,
    },
    /// R-051: a pipe WriteFile completed.
    PipeWriteDone {
        fut: Py<PyAny>,
        bytes: u32,
        err: u32,
    },
}

// --------------------------------------------------------------------- //
// in-cell machinery (no user Python, no Py drops)                       //
// --------------------------------------------------------------------- //

pub(crate) type Backend<'a> = &'a mut (dyn IoBackend + Send);

/// Post (or extend) the gather send for a transport. In-cell.
pub(crate) fn flush_pending(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, tid: u64) {
    let Some(entry) = net.transports.get_mut(&tid) else { return };
    entry.flush_scheduled = false;
    if entry.send_op.is_some() || entry.conn_lost {
        return;
    }
    if entry.wq.is_empty() {
        maybe_finish_shutdown(py, net, backend, tid);
        return;
    }
    let mut slices: [IoSlice; 16] = [IoSlice { ptr: std::ptr::null(), len: 0 }; 16];
    let mut n = 0;
    for buf in entry.wq.iter().take(16) {
        slices[n] = buf.slice();
        n += 1;
    }
    let socket = entry.socket;
    match backend.post_send(socket, &slices[..n]) {
        Ok(op) => {
            net.stats_sends_posted += 1;
            if let Some(entry) = net.transports.get_mut(&tid) {
                entry.send_op = Some(op);
            }
            net.ops.insert(op, OpTarget::Send(tid));
        }
        Err(e) => teardown_err(py, net, backend, tid, e),
    }
}

fn teardown_err(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, tid: u64, e: io::Error) {
    teardown_with(py, net, backend, tid, Some(e.raw_os_error().unwrap_or(5) as u32));
}

/// eof/close finalization once the write queue is drained. In-cell.
fn maybe_finish_shutdown(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, tid: u64) {
    let Some(entry) = net.transports.get_mut(&tid) else { return };
    if entry.send_op.is_some() || !entry.wq.is_empty() || entry.conn_lost {
        return;
    }
    if entry.eof_wanted && !entry.eof_sent {
        entry.eof_sent = true;
        let _ = netsys::shutdown_send(entry.socket);
    }
    if entry.closing {
        teardown_with(py, net, backend, tid, None);
    }
}

/// Cancel a stream transport's in-flight ops and keep every buffer the
/// kernel might still touch alive until the completions are reaped.
///
/// Two things must NOT happen here, and both used to:
///   * removing the op from `net.ops` — the completion then falls through
///     `dispatch_completions`'s `else { continue }`, so the recv slot's
///     op-reference (R-073) is never released and the slot leaks for the
///     life of the process;
///   * dropping the write queue with the entry — `post_send` points WSABUFs
///     straight at `WriteBuf` memory, so freeing it while an ABORTED
///     completion is still outstanding is a use-after-free.
fn cancel_transport_ops(net: &mut NetState, backend: Backend<'_>, entry: &mut TransportEntry) {
    if let Some(op) = entry.recv_op.take() {
        let _ = backend.cancel(op);
        // Mapping deliberately left in place: `on_recv_done` tolerates a
        // dead tid, and the dispatcher releases the op's slot reference.
    }
    if let Some(op) = entry.send_op.take() {
        let _ = backend.cancel(op);
        let wq: Vec<WriteBuf> = entry.wq.drain(..).collect();
        if !wq.is_empty() {
            net.reap_guards.insert(op, ReapGuard::Writes(wq));
        }
    }
    entry.queued_bytes = 0;
}

/// Same discipline for datagram endpoints. The recv slot has only ONE
/// reference (the entry's — `post_recv_from` takes no op reference), so it
/// is moved into the guard rather than released.
fn cancel_dgram_ops(net: &mut NetState, backend: Backend<'_>, entry: &mut DatagramEntry) {
    entry.send_queue.clear();
    entry.queued_bytes = 0;
    entry.recv_starved = false;
    if let Some(op) = entry.recv_op.take() {
        let _ = backend.cancel(op);
        if let Some(slot) = entry.recv_slot.take() {
            net.reap_guards.insert(op, ReapGuard::Slot(slot));
        }
    }
    if let Some(op) = entry.send_op.take() {
        // The payload was copied into the op slot by `post_send_to`, so the
        // slab keeps it alive; nothing of ours is still in kernel hands.
        let _ = backend.cancel(op);
    }
}

/// Tear a connection down: cancel ops, close the socket, emit ConnLost.
/// `err` None = orderly close. In-cell; entry moves to the graveyard.
pub(crate) fn teardown_with(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    tid: u64,
    err: Option<u32>,
) {
    let Some(mut entry) = net.transports.remove(&tid) else { return };
    if let Some(sock) = entry.extra_sock.take() {
        net.graveyard_sockets.push(sock);
    }
    if entry.conn_lost {
        net.graveyard_entries.push(entry);
        return;
    }
    entry.conn_lost = true;
    cancel_transport_ops(net, backend, &mut entry);
    backend.detach_socket(entry.socket);
    netsys::close(entry.socket);
    if let Some(slot) = entry.recv_slot.take() {
        // The transport's own reference. The posted op holds a second one
        // (R-073) that only the reap releases.
        net.buffers.release(slot);
    }
    match &mut entry.proto {
        ProtoKind::Py(p) => {
            net.events.push(NetEvent::ConnLost { connection_lost: p.connection_lost.clone_ref(py), err });
        }
        ProtoKind::Http(conn) => {
            conn.disconnected = true;
            let ws = conn.ws.is_some();
            if let Some(wsc) = conn.ws.as_ref() {
                // Outlives the entry so a receive() that arrives after
                // this teardown still answers `websocket.disconnect` with
                // the peer's code. 1006 = "closed abnormally, no close
                // frame", the honest answer when it never sent one.
                let code = wsc.close_code.unwrap_or(1006);
                net.note_ws_close(tid, code);
            }
            if let Some(fut) = conn.drain_waiter.take() {
                // The queue will never drain now; releasing the producer
                // lets its next send() raise properly instead of hanging.
                net.events.push(NetEvent::HttpDrained { fut });
            }
            if let Some(fut) = conn.take_recv_waiter() {
                net.events.push(NetEvent::HttpDisconnect { fut, ws });
            }
        }
    }
    if let Some(sock) = entry.extra_sock.take() {
        net.graveyard_sockets.push(sock);
    }
    net.graveyard_entries.push(entry);
}

/// Used by CoreLoop::close (no Python available for events — they are
/// cleared right after).
pub(crate) fn teardown(net: &mut NetState, backend: Backend<'_>, tid: u64, _err: Option<u32>) {
    let Some(mut entry) = net.transports.remove(&tid) else { return };
    entry.conn_lost = true;
    cancel_transport_ops(net, backend, &mut entry);
    backend.detach_socket(entry.socket);
    netsys::close(entry.socket);
    if let Some(slot) = entry.recv_slot.take() {
        net.buffers.release(slot);
    }
    if let Some(sock) = entry.extra_sock.take() {
        net.graveyard_sockets.push(sock);
    }
    net.graveyard_entries.push(entry);
}

/// How many torn-down WebSocket close codes to remember. A receive() that
/// races a teardown lands within a tick or two of it, so this only has to
/// outlive the handful of connections closed in that window.
const RECENT_WS_CLOSES: usize = 64;

impl NetState {
    /// Remember a WebSocket's close code as its connection goes away.
    fn note_ws_close(&mut self, tid: u64, code: u16) {
        if self.recent_ws_closes.len() == RECENT_WS_CLOSES {
            self.recent_ws_closes.pop_front();
        }
        self.recent_ws_closes.push_back((tid, code));
    }

    /// The close code of a WebSocket connection that has already been torn
    /// down, if it is still remembered.
    pub(crate) fn recent_ws_close(&self, tid: u64) -> Option<u16> {
        self.recent_ws_closes.iter().rev().find(|(t, _)| *t == tid).map(|(_, c)| *c)
    }
}

/// Cancel the ops that belong to no transport, listener or datagram
/// endpoint: an outstanding `connect()`, or a Windows named-pipe
/// read/write (R-051). Loop close walks the three maps, so these were
/// simply left behind -- their futures stayed pending, their pinned
/// buffers stayed alive, and a connect target kept an open socket, for as
/// long as the closed core existed.
///
/// Connect ops are settled here: their socket must be closed by someone,
/// and phase 2 (which normally does it) never runs again during close.
/// Pipe ops are only *requested* cancelled and deliberately left in
/// `net.ops`, so `reap_at_close` can release them properly when their
/// completion arrives; `sweep_unreaped_pipe_ops` handles the remainder.
///
/// Returns the Python references to drop OUTSIDE the cell (ADR-5).
pub(crate) fn cancel_standalone_ops(net: &mut NetState, backend: Backend<'_>) -> Vec<Py<PyAny>> {
    let standalone: Vec<OpId> = net
        .ops
        .iter()
        .filter(|(_, t)| {
            matches!(t, OpTarget::Connect { .. } | OpTarget::PipeRead { .. } | OpTarget::PipeWrite { .. })
        })
        .map(|(&op, _)| op)
        .collect();
    let mut dropped = Vec::with_capacity(standalone.len());
    for op in standalone {
        let _ = backend.cancel(op);
        if matches!(net.ops.get(&op), Some(OpTarget::Connect { .. })) {
            let Some(OpTarget::Connect { fut, sock }) = net.ops.remove(&op) else { unreachable!() };
            backend.detach_socket(sock);
            netsys::close(sock);
            dropped.push(fut);
        }
    }
    dropped
}

/// Pipe ops whose cancellation completion never arrived within the close
/// reap budget.
///
/// CancelIoEx only REQUESTS cancellation, and a pending ReadFile/WriteFile
/// keeps using its buffer until the completion is dequeued. Dropping the
/// Vec here -- which is what `cancel_standalone_ops` did when I first
/// wrote it -- is the same use-after-free the transport paths were fixed
/// for. Hand the whole target to the reaper instead: it outlives this
/// call, and its Python future is released with it, out of the cell.
fn sweep_unreaped_pipe_ops(net: &mut NetState) {
    let unreaped: Vec<OpId> = net
        .ops
        .iter()
        .filter(|(_, t)| matches!(t, OpTarget::PipeRead { .. } | OpTarget::PipeWrite { .. }))
        .map(|(&op, _)| op)
        .collect();
    for op in unreaped {
        if let Some(t) = net.ops.remove(&op) {
            net.closed_pipe_ops.push(t);
        }
    }
}

/// How long `reap_at_close` will wait for cancellation completions that
/// have not landed on the port yet.
///
/// Zero on the readiness backends, which push their ECANCELED completion
/// inline from `cancel()`/`detach_socket` -- one `try_poll` collects the
/// lot. IOCP posts the ABORTED packet asynchronously, so a short bounded
/// spin is the difference between releasing the buffers now and holding
/// them for the closed loop's whole remaining lifetime.
const CLOSE_REAP_BUDGET: Duration = Duration::from_millis(5);

/// Release the resources cancelled teardown handed to `reap_guards`.
///
/// Teardown cannot free a cancelled op's buffers (R-073: the kernel may
/// still be reading a send buffer or writing a recv slot until the op's
/// completion is dequeued), so it parks them in `reap_guards` and lets the
/// next `dispatch_completions` release them. A CLOSED loop never polls
/// again -- so without this, every queued response body a close cancelled
/// stayed resident for as long as anything referenced the dead loop. That
/// is up to `high_water` bytes per connection.
///
/// The discipline is unchanged, only the pump: poll the backend directly
/// and run the ordinary `translate`, so a guard is released on exactly the
/// same evidence as during a tick -- its completion came back. Guards
/// whose completion does not arrive inside the budget stay put and are
/// freed when the state (and with it the backend) drops, which is the
/// pre-existing behaviour. Nothing is freed early on any path.
///
/// In-cell, and Python-free: `translate` only moves refs into `events` /
/// the graveyards, which close drains afterwards.
pub(crate) fn reap_at_close(py: Python<'_>, net: &mut NetState, backend: Backend<'_>) {
    let mut comps: Vec<Completion> = Vec::new();
    let deadline = Instant::now() + CLOSE_REAP_BUDGET;
    loop {
        let pipes_left =
            net.ops.values().any(|t| matches!(t, OpTarget::PipeRead { .. } | OpTarget::PipeWrite { .. }));
        if net.reap_guards.is_empty() && !pipes_left {
            break;
        }
        comps.clear();
        let n = backend.try_poll(&mut comps).unwrap_or(0);
        if n > 0 {
            translate(py, net, backend, &comps);
        }
        // Checked on EVERY iteration, including the ones that translated
        // something: a stream of unrelated packets (a cross-thread Wakeup
        // being the easy one) must not keep this loop alive past its
        // budget while the guard it is waiting for never arrives.
        if Instant::now() >= deadline {
            break;
        }
        if n == 0 {
            std::thread::yield_now();
        }
    }
    sweep_unreaped_pipe_ops(net);
}

/// Queue native HTTP response bytes on a transport's corked write queue
/// (R-035/R-084: same-tick flush via flush_list). In-cell.
pub(crate) fn http_enqueue(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    tid: u64,
    data: Vec<u8>,
) {
    if data.is_empty() {
        return;
    }
    // R-059: TLS connections stage plaintext; a TlsFlush event encrypts
    // it (needs _ssl, so phase 2) and re-enters via http_enqueue_raw.
    if let Some(ProtoKind::Http(conn)) = net.transports.get_mut(&tid).map(|e| &mut e.proto) {
        if let Some(tls) = conn.tls.as_mut() {
            let first = tls.staged.is_empty();
            tls.staged.extend_from_slice(&data);
            if first {
                net.events.push(NetEvent::TlsFlush { tid });
            }
            return;
        }
    }
    http_enqueue_raw(py, net, backend, tid, data)
}

/// Bypass TLS staging: wire-ready bytes (ciphertext, or plaintext conns).
pub(crate) fn http_enqueue_raw(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    tid: u64,
    data: Vec<u8>,
) {
    if data.is_empty() {
        return;
    }
    let Some(entry) = net.transports.get_mut(&tid) else { return };
    if entry.conn_lost || entry.closing {
        return;
    }
    entry.queued_bytes += data.len();
    entry.wq.push_back(WriteBuf::Owned { data, off: 0 });
    let no_send = entry.send_op.is_none();
    let big = entry.queued_bytes >= CORK_FLUSH_BYTES;
    let unscheduled = !entry.flush_scheduled;
    if no_send {
        // This is the point where bytes become wire-ready, which for the
        // common response shape is once per response: `http.response.start`
        // only stashes the head, and the body message emits head+body as a
        // single buffer. So latency mode still costs one send per response,
        // not one per ASGI message.
        if big || net.flush_immediately {
            flush_pending(py, net, backend, tid);
        } else if unscheduled {
            if let Some(e) = net.transports.get_mut(&tid) {
                e.flush_scheduled = true;
            }
            net.flush_list.push(tid);
        }
    }
}

// --------------------------------------------------------------------- //
// datagram endpoints (R-058)                                            //
// --------------------------------------------------------------------- //

/// Wire a bound (and possibly connected) UDP socket as a datagram
/// endpoint. The engine owns the socket from here.
pub(crate) fn udp_wire(
    net: &mut NetState,
    backend: Backend<'_>,
    sock: RawSocket,
    datagram_received: Py<PyAny>,
    error_received: Py<PyAny>,
    connection_lost: Py<PyAny>,
) -> io::Result<u64> {
    backend.register_socket(sock)?;
    let did = net.next_id();
    net.datagrams.insert(
        did,
        DatagramEntry {
            socket: sock,
            datagram_received,
            error_received,
            connection_lost,
            recv_op: None,
            recv_slot: None,
            send_op: None,
            send_queue: VecDeque::new(),
            queued_bytes: 0,
            recv_starved: false,
            closing: false,
            conn_lost: false,
        },
    );
    if let Err(e) = dgram_post_recv(net, backend, did) {
        // The socket is registered and the endpoint is in the map, but no
        // id reaches the caller -- so nothing could ever close it. Roll the
        // whole thing back before reporting the failure.
        if let Some(mut entry) = net.datagrams.remove(&did) {
            cancel_dgram_ops(net, backend, &mut entry);
            backend.detach_socket(entry.socket);
            netsys::close(entry.socket);
            if let Some(slot) = entry.recv_slot.take() {
                net.buffers.release(slot);
            }
            net.graveyard_py.push(entry.datagram_received);
            net.graveyard_py.push(entry.error_received);
            net.graveyard_py.push(entry.connection_lost);
        }
        return Err(e);
    }
    Ok(did)
}

/// One outstanding 64 KiB recv per endpoint (max UDP datagram).
fn dgram_post_recv(net: &mut NetState, backend: Backend<'_>, did: u64) -> io::Result<()> {
    let Some(entry) = net.datagrams.get_mut(&did) else { return Ok(()) };
    if entry.closing || entry.conn_lost || entry.recv_op.is_some() {
        return Ok(());
    }
    let slot = entry.recv_slot.unwrap_or_else(|| net.buffers.acquire(SizeClass::S64K));
    let sock = entry.socket;
    let (ptr, len) = (net.buffers.slot_ptr(slot), SizeClass::S64K.size() as u32);
    ensure_buffers_registered(net, backend);
    match backend.post_recv_from(sock, ptr, len) {
        Ok(op) => {
            let entry = net.datagrams.get_mut(&did).unwrap();
            entry.recv_slot = Some(slot);
            entry.recv_op = Some(op);
            net.ops.insert(op, OpTarget::DgramRecv(did));
            Ok(())
        }
        Err(e) => {
            net.buffers.release(slot);
            if let Some(entry) = net.datagrams.get_mut(&did) {
                entry.recv_slot = None;
            }
            Err(e)
        }
    }
}

fn on_dgram_recv_done(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    did: u64,
    op: OpId,
    bytes: u32,
    os_error: u32,
) {
    let addr = if os_error == 0 { backend.take_recv_from_addr(op) } else { None };
    let Some(entry) = net.datagrams.get_mut(&did) else { return };
    entry.recv_op = None;
    if entry.closing || entry.conn_lost {
        return;
    }
    if os_error == 0 {
        let slot = entry.recv_slot.expect("recv op had a slot");
        let datagram_received = entry.datagram_received.clone_ref(py);
        let ptr = net.buffers.slot_ptr(slot);
        let payload = unsafe {
            let obj = ffi::PyBytes_FromStringAndSize(ptr.cast(), bytes as ffi::Py_ssize_t);
            Bound::from_owned_ptr(py, obj).unbind()
        };
        net.events.push(NetEvent::DgramData { datagram_received, payload, addr });
    } else if !is_cancelled_error(os_error) {
        // Per-packet errors (e.g. ECONNREFUSED surfaced by a connected
        // socket after an ICMP unreachable): report and KEEP receiving —
        // asyncio semantics.
        let error_received = entry.error_received.clone_ref(py);
        net.events.push(NetEvent::DgramError { error_received, err: os_error });
    } else {
        return; // cancelled: teardown owns the endpoint
    }
    if dgram_post_recv(net, backend, did).is_err() {
        // Transient (descriptor or kernel-resource exhaustion). With no
        // recv outstanding no completion can ever retry this, so the
        // endpoint would stay open and silently deaf; flag it and let the
        // tick re-arm, exactly as a starved accept pool does.
        if let Some(e) = net.datagrams.get_mut(&did) {
            e.recv_starved = true;
            net.any_starved_listener = true;
            net.stats_accept_starved += 1;
        }
    }
}

fn on_dgram_send_done(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    did: u64,
    _op: OpId,
    os_error: u32,
) {
    let Some(entry) = net.datagrams.get_mut(&did) else { return };
    entry.send_op = None;
    if os_error != 0 && !is_cancelled_error(os_error) {
        let error_received = entry.error_received.clone_ref(py);
        net.events.push(NetEvent::DgramError { error_received, err: os_error });
    }
    dgram_pump_send(py, net, backend, did);
}

/// Post queued datagrams until one is actually in flight or the queue is
/// empty, then honour a deferred close.
///
/// A synchronous `post_send_to` failure is a per-packet error, not an
/// endpoint failure -- but it also means no completion is coming for that
/// packet. Returning after one failed post therefore stranded every
/// remaining datagram behind it (send_op is None, so nothing re-enters
/// here) and skipped the deferred-close check, leaving the endpoint open
/// for good.
fn dgram_pump_send(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, did: u64) {
    loop {
        let Some(entry) = net.datagrams.get_mut(&did) else { return };
        if entry.send_op.is_some() {
            return; // a send is in flight; its completion resumes us
        }
        let Some((data, addr)) = entry.send_queue.pop_front() else { break };
        entry.queued_bytes = entry.queued_bytes.saturating_sub(data.len());
        dgram_send_now(py, net, backend, did, &data, addr.as_ref());
    }
    let Some(entry) = net.datagrams.get_mut(&did) else { return };
    if entry.closing && !entry.conn_lost && entry.send_op.is_none() {
        udp_teardown(py, net, backend, did, None);
    }
}

fn dgram_send_now(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    did: u64,
    data: &[u8],
    addr: Option<&std::net::SocketAddr>,
) {
    let Some(entry) = net.datagrams.get_mut(&did) else { return };
    let sock = entry.socket;
    match backend.post_send_to(sock, data, addr) {
        Ok(op) => {
            net.datagrams.get_mut(&did).unwrap().send_op = Some(op);
            net.ops.insert(op, OpTarget::DgramSend(did));
        }
        Err(e) => {
            // Synchronous refusal: per-packet error, endpoint stays up.
            // The caller keeps draining -- no completion is coming for
            // this packet, so it must not be treated as "in flight".
            if let Some(entry) = net.datagrams.get_mut(&did) {
                let error_received = entry.error_received.clone_ref(py);
                net.events
                    .push(NetEvent::DgramError { error_received, err: e.raw_os_error().unwrap_or(0) as u32 });
            }
        }
    }
}

/// asyncio DatagramTransport.sendto: fire-and-forget; sends serialize
/// through one in-flight op with a queue behind it.
pub(crate) fn udp_sendto(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    did: u64,
    data: &[u8],
    addr: Option<std::net::SocketAddr>,
) -> io::Result<()> {
    let Some(entry) = net.datagrams.get_mut(&did) else {
        return Err(io::Error::new(io::ErrorKind::NotFound, "datagram endpoint closed"));
    };
    if entry.closing || entry.conn_lost {
        return Err(io::Error::new(io::ErrorKind::NotFound, "datagram endpoint closing"));
    }
    if entry.send_op.is_some() {
        if entry.queued_bytes + data.len() > DGRAM_SEND_QUEUE_MAX {
            // Deterministic overflow policy: drop and report. Growing
            // without bound is the alternative, and a producer that can
            // outrun the socket would take the process down with it.
            let error_received = entry.error_received.clone_ref(py);
            net.events.push(NetEvent::DgramError { error_received, err: ENOBUFS });
            return Ok(());
        }
        entry.queued_bytes += data.len();
        entry.send_queue.push_back((data.to_vec(), addr));
        return Ok(());
    }
    dgram_send_now(py, net, backend, did, data, addr.as_ref());
    Ok(())
}

/// Swap a datagram endpoint's cached protocol callbacks (R-058).
///
/// `udp_open` caches BOUND methods of the protocol object, so
/// `set_protocol()` changing only the Python attribute left every
/// datagram and send error going to the old protocol while
/// `get_protocol()` reported the new one. Displaced references drop via
/// the graveyard, never in-cell (ADR-5).
pub(crate) fn udp_set_callbacks(
    net: &mut NetState,
    did: u64,
    datagram_received: Py<PyAny>,
    error_received: Py<PyAny>,
) -> bool {
    let Some(entry) = net.datagrams.get_mut(&did) else { return false };
    let old_dr = std::mem::replace(&mut entry.datagram_received, datagram_received);
    let old_er = std::mem::replace(&mut entry.error_received, error_received);
    net.graveyard_py.push(old_dr);
    net.graveyard_py.push(old_er);
    true
}

/// Bytes queued behind the in-flight send, for the transport's
/// `get_write_buffer_size()`.
pub(crate) fn udp_queued_bytes(net: &NetState, did: u64) -> usize {
    net.datagrams.get(&did).map(|e| e.queued_bytes).unwrap_or(0)
}

/// close() flushes queued sends first; abort() drops them (asyncio
/// semantics). connection_lost fires exactly once via the event queue.
pub(crate) fn udp_close(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, did: u64, abort: bool) {
    let Some(entry) = net.datagrams.get_mut(&did) else { return };
    if entry.conn_lost {
        return;
    }
    entry.closing = true;
    if abort {
        entry.send_queue.clear();
    }
    if entry.send_op.is_none() && entry.send_queue.is_empty() {
        udp_teardown(py, net, backend, did, None);
    }
    // else: on_dgram_send_done finishes the close once the queue drains.
}

/// Loop-close variant: no Python events (they are cleared right after);
/// refs drop via the graveyard outside the cell.
pub(crate) fn udp_teardown_at_close(net: &mut NetState, backend: Backend<'_>, did: u64) {
    let Some(mut entry) = net.datagrams.remove(&did) else { return };
    cancel_dgram_ops(net, backend, &mut entry);
    backend.detach_socket(entry.socket);
    netsys::close(entry.socket);
    if let Some(slot) = entry.recv_slot.take() {
        net.buffers.release(slot);
    }
    net.graveyard_py.push(entry.datagram_received);
    net.graveyard_py.push(entry.error_received);
    net.graveyard_py.push(entry.connection_lost);
}

fn udp_teardown(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, did: u64, err: Option<u32>) {
    let Some(mut entry) = net.datagrams.remove(&did) else { return };
    if entry.conn_lost {
        return;
    }
    entry.conn_lost = true;
    cancel_dgram_ops(net, backend, &mut entry);
    backend.detach_socket(entry.socket);
    netsys::close(entry.socket);
    if let Some(slot) = entry.recv_slot.take() {
        net.buffers.release(slot);
    }
    net.events.push(NetEvent::DgramLost { connection_lost: entry.connection_lost.clone_ref(py), err });
    net.graveyard_py.push(entry.datagram_received);
    net.graveyard_py.push(entry.error_received);
    net.graveyard_py.push(entry.connection_lost);
}

/// R-080 connection-timeout sweep. Called periodically (the facade arms a
/// coarse repeating timer); walks HTTP connections and enforces the two
/// windows: request-head receipt (anchored at head START, so drip-fed
/// bytes cannot extend it — slowloris) and keep-alive idle (re-anchored
/// whenever activity moved between sweeps). Busy connections (app
/// running, queued pipeline, response streaming) are never timed out
/// here. Returns (head_timeouts, idle_closes).
/// Work that a shutdown or an idle sweep must not interrupt.
///
/// ONE definition, deliberately. `http_begin_shutdown` used to carry its
/// own copy under a comment claiming it matched this one -- and when the
/// draining case was added here it was not added there, so the graceful
/// drain tore down connections whose response bytes were still queued,
/// truncating exactly the responses it exists to protect. A comment is
/// not a mechanism; a shared function is.
fn http_conn_busy(entry: &TransportEntry) -> bool {
    // Bytes queued or in flight: the application is finished but the
    // response is not.
    if !entry.wq.is_empty() || entry.send_op.is_some() {
        return true;
    }
    let ProtoKind::Http(conn) = &entry.proto else { return false };
    // Done/Idle are between-requests states, so only a live app, a queued
    // pipeline, or a mid-flight response counts.
    conn.active
        || !conn.pending.is_empty()
        || matches!(conn.resp, crate::http::RespPhase::Started | crate::http::RespPhase::Streaming)
}

pub(crate) fn http_sweep(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    now_ns: u64,
) -> (u32, u32) {
    let mut expire_head: Vec<u64> = Vec::new();
    let mut expire_idle: Vec<u64> = Vec::new();
    for (&tid, entry) in net.transports.iter_mut() {
        if entry.conn_lost || entry.closing {
            continue;
        }
        // A response whose last ASGI message has been produced is not
        // finished: its bytes may still be queued or in flight. Counting
        // that connection idle started the keep-alive clock while the
        // response was still going out, so a large body to a slow client
        // could be torn down mid-transmission by the idle sweep.
        let busy = http_conn_busy(entry);
        let ProtoKind::Http(conn) = &mut entry.proto else { continue };
        let phase: u8 = if busy {
            2
        } else if conn.parser.in_head() {
            1
        } else {
            0
        };
        let moved = conn.activity != conn.sweep_seen;
        conn.sweep_seen = conn.activity;
        if phase != conn.sweep_phase || conn.sweep_anchor_ns == 0 || phase == 2 || (phase == 0 && moved) {
            // New phase, first sighting, busy, or fresh idle activity:
            // (re)anchor. A head in progress deliberately does NOT
            // re-anchor on activity — that is the slowloris rule.
            conn.sweep_phase = phase;
            conn.sweep_anchor_ns = now_ns;
            continue;
        }
        let elapsed = now_ns.saturating_sub(conn.sweep_anchor_ns);
        match phase {
            1 if conn.head_timeout_ns > 0 && elapsed >= conn.head_timeout_ns => expire_head.push(tid),
            0 if conn.idle_timeout_ns > 0 && elapsed >= conn.idle_timeout_ns => expire_idle.push(tid),
            _ => {}
        }
    }
    for &tid in &expire_head {
        // 408 then close: the head never completed inside the window.
        let resp = crate::http::error_response(ParseError { status: 408, reason: "Request Timeout" });
        http_enqueue(py, net, backend, tid, resp);
        http_close_after_write(py, net, backend, tid);
    }
    for &tid in &expire_idle {
        // Idle keep-alive expiry closes silently (uvicorn-compatible).
        teardown_with(py, net, backend, tid, None);
    }
    (expire_head.len() as u32, expire_idle.len() as u32)
}

/// Graceful shutdown, phase 1 (R-092): end keep-alive everywhere, close
/// what is idle right now, start the closing handshake on live
/// WebSockets, and report how many connections still have work in
/// flight. In-cell.
///
/// The listener is already closed by the time this runs, so no connection
/// arrives after it. What remains either finishes on its own -- with
/// `keep_alive` cleared, `finish_request` closes each one as its response
/// completes -- or is still open when the caller's grace deadline expires
/// and gets torn down with everything else.
///
/// Without this, shutdown went straight to `CoreLoop::close()`, which
/// cancels every in-flight operation: a response half-written to the wire
/// was simply truncated, and a WebSocket peer saw a bare TCP close instead
/// of a close frame.
pub(crate) fn http_begin_shutdown(py: Python<'_>, net: &mut NetState, backend: Backend<'_>) -> usize {
    let mut idle: Vec<u64> = Vec::new();
    let mut ws_closing: Vec<u64> = Vec::new();
    let mut busy = 0usize;
    for (&tid, entry) in net.transports.iter_mut() {
        if entry.conn_lost || entry.closing {
            continue;
        }
        let busy_http = http_conn_busy(entry);
        let ProtoKind::Http(conn) = &mut entry.proto else { continue };
        conn.keep_alive = false;
        match conn.ws.as_ref() {
            // A WebSocket never finishes by itself -- it has to be told.
            // 1012 (service restart) is what the close frame carries.
            Some(wsc) if wsc.accepted && !wsc.closing => {
                ws_closing.push(tid);
                busy += 1;
            }
            Some(_) => busy += 1,
            None if busy_http => busy += 1,
            None => idle.push(tid),
        }
    }
    for tid in ws_closing {
        if let Some(conn) = net.http_conn_mut(tid) {
            if let Some(wsc) = conn.ws.as_mut() {
                wsc.closing = true;
                wsc.inbox.push_back(crate::http::WsMsg::Disconnect(1012));
            }
        }
        http_enqueue(py, net, backend, tid, cadeloop_core::ws::close_frame(1012, "server shutdown"));
        http_close_after_write(py, net, backend, tid);
    }
    for tid in idle {
        teardown_with(py, net, backend, tid, None);
    }
    busy
}

/// Mark a native HTTP connection to close once its write queue drains
/// (`connection: close`, parse errors, app failures). In-cell.
pub(crate) fn http_close_after_write(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, tid: u64) {
    let Some(entry) = net.transports.get_mut(&tid) else { return };
    let healthy = !entry.conn_lost && !entry.closing;
    // R-059, two reasons to defer a TLS connection's shutdown:
    //   1. staged plaintext must reach the wire first, and
    //   2. the session owes the peer a `close_notify` alert -- without it
    //      the peer sees the TCP connection end mid-session and a strict
    //      client reports SSLEOFError even though the response that came
    //      before it was complete.
    // Both are handled by the same TlsFlush round trip, which runs
    // outside the state cell where the SSLObject can actually be called;
    // it comes back here with `shutdown_sent` set and falls through.
    if let ProtoKind::Http(conn) = &mut entry.proto {
        if let Some(tls) = conn.tls.as_mut() {
            if !tls.staged.is_empty() {
                tls.close_after = true;
                return;
            }
            if healthy && !tls.shutdown_sent {
                tls.close_after = true;
                net.events.push(NetEvent::TlsFlush { tid });
                return;
            }
        }
    }
    if !healthy {
        return;
    }
    entry.closing = true;
    if let Some(op) = entry.recv_op.take() {
        // Mapping stays; the completion is discarded by the closing check.
        let _ = backend.cancel(op);
    }
    maybe_finish_shutdown(py, net, backend, tid);
}

/// R-043: after slab growth, hand the new regions to the backend for
/// registration (RIORegisterBuffer on RIO; no-op elsewhere). In-cell.
fn ensure_buffers_registered(net: &mut NetState, backend: Backend<'_>) {
    if !net.buffers.take_regions_dirty() {
        return;
    }
    let mut regions = net.buffers.unregistered_regions_mut();
    if !regions.is_empty() {
        // Failure leaves cookies unset; RIO post_recv will then refuse the
        // buffer and the connection errors visibly (never silent loss).
        let _ = backend.register_buffers(&mut regions);
    }
}

/// Post the next recv on a transport. In-cell.
fn post_recv(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, tid: u64) {
    let Some(entry) = net.transports.get_mut(&tid) else { return };
    if entry.recv_op.is_some() || entry.reading_paused || entry.closing || entry.conn_lost {
        return;
    }
    // Engine-level backpressure is checked HERE rather than at each call
    // site: the TLS path reposts from its own branch, so a flag consulted
    // only by the plaintext caller left HTTPS connections pipelining
    // without any bound at all (R-085/R-087).
    if let ProtoKind::Http(conn) = &entry.proto {
        if conn.pipeline_paused || conn.ws.as_ref().is_some_and(|w| w.inbox_paused) {
            return;
        }
    }
    let slot = match entry.recv_slot {
        Some(s) => s,
        None => {
            let s = net.buffers.acquire(RECV_CLASS);
            ensure_buffers_registered(net, backend);
            net.transports.get_mut(&tid).unwrap().recv_slot = Some(s);
            s
        }
    };
    let entry = net.transports.get_mut(&tid).unwrap();
    let socket = entry.socket;
    let ptr = net.buffers.slot_ptr(slot);
    let len = net.buffers.slot_len(slot) as u32;
    match backend.post_recv(socket, ptr, len) {
        Ok(op) => {
            // R-073: the kernel op holds its own reference — the slot must
            // not recycle until this op's completion is reaped, even if the
            // transport is torn down first (IOCP may write until the
            // ABORTED completion arrives).
            net.buffers.retain(slot);
            net.transports.get_mut(&tid).unwrap().recv_op = Some(op);
            net.ops.insert(op, OpTarget::Recv { tid, slot });
        }
        Err(e) => teardown_err(py, net, backend, tid, e),
    }
}

/// What the live kernel operations actually are, by target kind.
///
/// A bare `ops=66` cannot distinguish "a connect completion was never
/// delivered" from "a recv was orphaned on a dead transport", and that
/// distinction is what made the R-073 class of hang diagnosable at all.
/// Written for a temporary trace (ADR-24); kept and promoted to
/// `stats()` because a loop that is stuck looks completely healthy on
/// every other counter, and this is the one that says where.
pub(crate) fn op_breakdown(net: &NetState) -> OpBreakdown {
    let mut b = OpBreakdown::default();
    for t in net.ops.values() {
        match t {
            OpTarget::Recv { .. } => b.recv += 1,
            OpTarget::Send(_) => b.send += 1,
            OpTarget::Accept(_) => b.accept += 1,
            OpTarget::Connect { .. } => b.connect += 1,
            OpTarget::DgramRecv(_) | OpTarget::DgramSend(_) => b.dgram += 1,
            OpTarget::PipeRead { .. } | OpTarget::PipeWrite { .. } => b.pipe += 1,
        }
    }
    b
}

#[derive(Default)]
pub(crate) struct OpBreakdown {
    pub recv: usize,
    pub send: usize,
    pub accept: usize,
    pub connect: usize,
    pub dgram: usize,
    pub pipe: usize,
}

impl NetState {
    /// Outstanding `AcceptEx`/`accept4` operations across all listeners.
    /// Zero with a live listener means the listener is deaf.
    pub(crate) fn accept_ops_outstanding(&self) -> usize {
        self.listeners.values().map(|l| l.accept_ops.len()).sum()
    }
}

pub(crate) fn listener_create(net: &mut NetState, sock: RawSocket, kind: ListenerKind, target: usize) -> u64 {
    let lid = net.next_id();
    net.listeners.insert(
        lid,
        ListenerEntry {
            socket: sock,
            kind,
            accept_ops: Vec::new(),
            target: target.max(1),
            closing: false,
            starved: false,
        },
    );
    lid
}

/// Arm a listener's accept pool. Fails loudly when not a single accept
/// could be posted: a listener with nothing outstanding accepts nothing,
/// ever, and returning "serving" for it hides the outage completely.
pub(crate) fn listener_start(net: &mut NetState, backend: Backend<'_>, lid: u64) -> io::Result<()> {
    post_accepts(net, backend, lid);
    match net.listeners.get(&lid) {
        Some(l) if l.accept_ops.is_empty() => {
            Err(io::Error::other("listener could not post its first accept"))
        }
        _ => Ok(()),
    }
}

/// Re-arm listeners whose accept pool ran dry on a transient failure.
/// Driven from the tick, because a starved listener has no outstanding
/// operation left to deliver the completion that would otherwise retry.
pub(crate) fn retry_starved_listeners(net: &mut NetState, backend: Backend<'_>) {
    if !net.any_starved_listener {
        return;
    }
    let starved: Vec<u64> =
        net.listeners.iter().filter(|(_, l)| l.starved && !l.closing).map(|(&lid, _)| lid).collect();
    let starved_dgrams: Vec<u64> = net
        .datagrams
        .iter()
        .filter(|(_, e)| e.recv_starved && !e.closing && !e.conn_lost)
        .map(|(&did, _)| did)
        .collect();
    net.any_starved_listener = false;
    for lid in starved {
        post_accepts(net, backend, lid);
    }
    for did in starved_dgrams {
        match dgram_post_recv(net, backend, did) {
            Ok(()) => {
                if let Some(e) = net.datagrams.get_mut(&did) {
                    e.recv_starved = false;
                }
            }
            Err(_) => net.any_starved_listener = true, // try again next tick
        }
    }
}

pub(crate) fn listener_teardown(net: &mut NetState, backend: Backend<'_>, lid: u64) {
    let Some(mut listener) = net.listeners.remove(&lid) else { return };
    listener.closing = true;
    for op in listener.accept_ops.drain(..) {
        let _ = backend.cancel(op);
        // Mapping kept: `on_accept_done` already knows how to reap a
        // socket AcceptEx produced for a listener that has since closed,
        // but only if the completion still resolves to this lid.
    }
    backend.detach_socket(listener.socket);
    netsys::close(listener.socket);
    match listener.kind {
        ListenerKind::Factory(factory) => net.graveyard_py.push(factory),
        ListenerKind::Http { app, pyloop, state, .. } => {
            net.graveyard_py.push(app);
            net.graveyard_py.push(pyloop);
            net.graveyard_py.push(state);
        }
    }
}

fn post_accepts(net: &mut NetState, backend: Backend<'_>, lid: u64) {
    loop {
        let Some(listener) = net.listeners.get_mut(&lid) else { return };
        if listener.closing || listener.accept_ops.len() >= listener.target {
            listener.starved = false;
            return;
        }
        let socket = listener.socket;
        match backend.post_accept(socket) {
            Ok(op) => {
                let l = net.listeners.get_mut(&lid).unwrap();
                l.accept_ops.push(op);
                l.starved = false;
                net.ops.insert(op, OpTarget::Accept(lid));
            }
            Err(_) => {
                // Transient (a descriptor limit, say). With at least one
                // accept still outstanding its completion retries this.
                // With none, nothing ever would -- the listener would go
                // permanently deaf while still reporting itself as
                // serving -- so flag it for the tick to re-arm.
                let l = net.listeners.get_mut(&lid).unwrap();
                if l.accept_ops.is_empty() {
                    l.starved = true;
                    net.any_starved_listener = true;
                    net.stats_accept_starved += 1;
                }
                return;
            }
        }
    }
}

/// Translate one poll's completions into phase-2 events. In-cell.
pub(crate) fn translate(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    completions: &[Completion],
) {
    for &c in completions {
        match c {
            Completion::Wakeup => {}
            Completion::Ready { socket, readable, writable } => {
                if readable {
                    if let Some(h) = net.readers.get(&socket) {
                        let h = h.clone_ref(py);
                        net.ready_scratch.push(h);
                    }
                }
                if writable {
                    if let Some(h) = net.writers.get(&socket) {
                        let h = h.clone_ref(py);
                        net.ready_scratch.push(h);
                    }
                }
            }
            Completion::Io { op, bytes, os_error } => {
                // Before anything else, and before the `continue` below:
                // the kernel is done with this op, so any buffers held
                // back for it can finally be freed.
                if let Some(guard) = net.reap_guards.remove(&op) {
                    match guard {
                        ReapGuard::Slot(slot) => {
                            net.buffers.release(slot);
                        }
                        // ADR-5: a `WriteBuf::Bytes` decref must not run
                        // inside the cell.
                        ReapGuard::Writes(bufs) => net.graveyard_bufs.extend(bufs),
                    }
                }
                let Some(target) = net.ops.remove(&op) else { continue };
                match target {
                    OpTarget::Recv { tid, slot } => {
                        on_recv_done(py, net, backend, tid, op, slot, bytes, os_error);
                        net.buffers.release(slot); // op's ref (R-073)
                    }
                    OpTarget::Send(tid) => on_send_done(py, net, backend, tid, op, bytes, os_error),
                    OpTarget::Accept(lid) => on_accept_done(net, backend, lid, op, os_error),
                    OpTarget::Connect { fut, sock } => {
                        net.events.push(NetEvent::ConnectDone { fut, sock, err: os_error });
                    }
                    OpTarget::DgramRecv(did) => {
                        on_dgram_recv_done(py, net, backend, did, op, bytes, os_error);
                    }
                    OpTarget::DgramSend(did) => {
                        on_dgram_send_done(py, net, backend, did, op, os_error);
                    }
                    OpTarget::PipeRead { fut, buf } => {
                        net.events.push(NetEvent::PipeReadDone { fut, buf, bytes, err: os_error });
                    }
                    OpTarget::PipeWrite { fut, _buf } => {
                        net.events.push(NetEvent::PipeWriteDone { fut, bytes, err: os_error });
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn on_recv_done(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    tid: u64,
    op: OpId,
    slot: SlotId,
    bytes: u32,
    os_error: u32,
) {
    let mut pipeline_pause = false;
    let mut paused_now = false;
    let Some(entry) = net.transports.get_mut(&tid) else { return };
    if entry.recv_op == Some(op) {
        entry.recv_op = None;
    }
    if entry.conn_lost || entry.closing {
        return; // inbound data after close(): discarded by design
    }
    if os_error != 0 {
        if is_cancelled_error(os_error) {
            return; // pause_reading()/close() cancelled it
        }
        teardown_with(py, net, backend, tid, Some(os_error));
        return;
    }
    if bytes == 0 {
        // Peer EOF. Recv is NOT re-posted.
        let (http_idle, waiter) = match &mut entry.proto {
            ProtoKind::Py(p) => {
                // Phase 2 decides close-vs-keep from eof_received().
                let ev = NetEvent::Eof {
                    eof_received: p.eof_received.clone_ref(py),
                    transport: entry
                        .pyobj
                        .as_ref()
                        .expect("py protocol entries always carry a Transport")
                        .clone_ref(py),
                };
                net.events.push(ev);
                return;
            }
            ProtoKind::Http(conn) => {
                // Idle keep-alive connection: tear down now. Mid-request:
                // keep writing the response; finish_request closes it.
                conn.disconnected = true;
                let ws = conn.ws.is_some();
                ((!conn.active && conn.pending.is_empty(), ws), conn.take_recv_waiter())
            }
        };
        let (http_idle, waiter_ws) = (http_idle.0, http_idle.1);
        if let Some(fut) = waiter {
            net.events.push(NetEvent::HttpDisconnect { fut, ws: waiter_ws });
        }
        if http_idle {
            teardown_with(py, net, backend, tid, None);
        }
        return;
    }
    net.stats_bytes_rx += bytes as u64;
    debug_assert_eq!(entry.recv_slot, Some(slot), "op slot / transport slot mismatch");
    let mut parse_err = None;
    let mut continue_owed = false;
    if entry.reading_paused && matches!(entry.proto, ProtoKind::Py(_)) {
        // A protocol that called pause_reading() did so to stop consuming;
        // the read already in flight when it paused must not be delivered
        // anyway, or a fast peer hands it another full buffer past the
        // limit that prompted the pause. Held here, delivered by
        // resume_reading. (Cancelling instead would race the completion
        // and the slot reuse, and on Windows the bytes have already left
        // the kernel buffer, so they would simply be lost.)
        let ptr = net.buffers.slot_ptr(slot);
        let data = unsafe { std::slice::from_raw_parts(ptr, bytes as usize) };
        match &mut entry.paused_recv {
            Some(held) => held.extend_from_slice(data),
            None => entry.paused_recv = Some(data.to_vec()),
        }
        entry.recv_slot = None;
        net.buffers.release(slot);
        return;
    }
    match &mut entry.proto {
        ProtoKind::Py(p) => {
            if let Some(data_received) = &p.data_received {
                // Plain Protocol: materialize bytes now (non-GC-tracked
                // allocation, safe in-cell) and re-post into the SAME slot
                // immediately so the kernel refills while Python processes.
                let data_received = data_received.clone_ref(py);
                let ptr = net.buffers.slot_ptr(slot);
                let payload = unsafe {
                    let obj = ffi::PyBytes_FromStringAndSize(ptr.cast(), bytes as ffi::Py_ssize_t);
                    Bound::from_owned_ptr(py, obj).unbind()
                };
                net.events.push(NetEvent::Data { tid, data_received, payload });
            } else {
                // BufferedProtocol: the transport's slot reference transfers
                // to the phase-2 event (released there after the copy); a
                // fresh slot is acquired for the next read.
                let get_buffer = p.get_buffer.as_ref().unwrap().clone_ref(py);
                let buffer_updated = p.buffer_updated.as_ref().unwrap().clone_ref(py);
                entry.recv_slot = None; // ref now owned by the BufData event
                net.events.push(NetEvent::BufData {
                    tid,
                    get_buffer,
                    buffer_updated,
                    slot,
                    len: bytes as usize,
                });
            }
        }
        ProtoKind::Http(conn) => {
            let ptr = net.buffers.slot_ptr(slot);
            let data = unsafe { std::slice::from_raw_parts(ptr, bytes as usize) };
            if conn.tls.is_some() {
                // R-059: ciphertext — record processing needs _ssl (phase 2).
                net.events.push(NetEvent::TlsData { tid, data: data.to_vec() });
            } else if conn.ws.is_some() {
                // R-087: WS mode — bytes go to the frame parser, not llhttp.
                // Copied out first: ws_ingest needs &mut NetState.
                let owned = data.to_vec();
                crate::http::ws_ingest(py, net, backend, tid, &owned);
            } else {
                // In-cell parse (R-080): llhttp over the recv slot; the
                // parser copies what it keeps, so the slot re-posts below.
                match crate::http::conn_feed(conn, data) {
                    Ok(outcome) => {
                        // R-085: stop reading while the pipeline budget is
                        // spent. The connection is flagged here and the
                        // recv is simply not reposted below; pump_requests
                        // resumes it as the queue drains.
                        if outcome.pause_reading && !conn.pipeline_paused {
                            conn.pipeline_paused = true;
                            pipeline_pause = true;
                            paused_now = true;
                        } else if outcome.pause_reading {
                            pipeline_pause = true;
                        }
                        if outcome.pump {
                            net.events.push(NetEvent::HttpPump { tid });
                        }
                        if outcome.owes_continue {
                            continue_owed = true;
                        }
                    }
                    Err(e) => parse_err = Some(e),
                }
            }
        }
    }
    if let Some(err) = parse_err {
        // R-086: malformed request answered entirely in-cell, then closed.
        let resp = crate::http::error_response(err);
        http_enqueue(py, net, backend, tid, resp);
        http_close_after_write(py, net, backend, tid);
        return; // no recv re-post
    }
    if continue_owed {
        // Before the body, not after: the client is holding it back until
        // this arrives. A request whose declared length already exceeds
        // max_body never gets here -- the parser answers 413 at
        // headers-complete instead, which is the other half of what the
        // expectation is for.
        http_enqueue(py, net, backend, tid, crate::http::CONTINUE_RESPONSE.to_vec());
    }
    if paused_now {
        net.stats_pipeline_pauses += 1;
    }
    if pipeline_pause {
        return; // budget spent; http_resume_reading re-posts
    }
    post_recv(py, net, backend, tid);
}

/// Deliver bytes a paused transport held, if any. In-cell; the actual
/// protocol call happens in phase 2 like every other receive.
fn flush_paused_recv(py: Python<'_>, net: &mut NetState, tid: u64) {
    let Some(entry) = net.transports.get_mut(&tid) else { return };
    if entry.conn_lost || entry.closing {
        entry.paused_recv = None;
        return;
    }
    let Some(data) = entry.paused_recv.take() else { return };
    let ProtoKind::Py(p) = &entry.proto else { return };
    if let Some(data_received) = &p.data_received {
        let payload = unsafe {
            let obj = ffi::PyBytes_FromStringAndSize(data.as_ptr().cast(), data.len() as ffi::Py_ssize_t);
            Bound::from_owned_ptr(py, obj).unbind()
        };
        let data_received = data_received.clone_ref(py);
        net.events.push(NetEvent::Data { tid, data_received, payload });
    } else if let (Some(get_buffer), Some(buffer_updated)) = (&p.get_buffer, &p.buffer_updated) {
        net.events.push(NetEvent::BufDataOwned {
            tid,
            get_buffer: get_buffer.clone_ref(py),
            buffer_updated: buffer_updated.clone_ref(py),
            data,
        });
    }
}

/// Stop reading on a transport for engine-level backpressure (R-085/
/// R-087). Distinct from the user-facing `pause_reading()`: the engine
/// owns this flag and releases it itself.
pub(crate) fn pause_reading_for_backpressure(net: &mut NetState, tid: u64) {
    if let Some(e) = net.transports.get_mut(&tid) {
        e.reading_paused = true;
    }
}

/// The matching release: re-post the recv if nothing else is holding it.
pub(crate) fn resume_reading_after_backpressure(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    tid: u64,
) {
    if let Some(e) = net.transports.get_mut(&tid) {
        if !e.reading_paused {
            return;
        }
        e.reading_paused = false;
    }
    flush_paused_recv(py, net, tid);
    post_recv(py, net, backend, tid);
}

/// Queued write bytes and the high-water mark for a transport (R-084).
///
/// TLS connections stage PLAINTEXT in `TlsState.staged` and only move it
/// to the wire queue on a later TlsFlush, so `queued_bytes` alone reads
/// as zero for HTTPS/WSS however much the producer has written -- the
/// backpressure would have been inert on exactly the connections whose
/// encryption makes them slowest. Count both.
pub(crate) fn write_pressure(net: &NetState, tid: u64) -> Option<(usize, usize)> {
    net.transports.get(&tid).map(|e| {
        let staged = match &e.proto {
            ProtoKind::Http(conn) => conn.tls.as_ref().map(|t| t.staged.len()).unwrap_or(0),
            ProtoKind::Py(_) => 0,
        };
        (e.queued_bytes + staged, e.high_water)
    })
}

/// Park an ASGI producer on this connection until the queue drains.
pub(crate) fn set_drain_waiter(net: &mut NetState, tid: u64, fut: Py<PyAny>) -> Option<Py<PyAny>> {
    match net.transports.get_mut(&tid).map(|e| &mut e.proto) {
        Some(ProtoKind::Http(conn)) => conn.drain_waiter.replace(fut),
        _ => Some(fut), // no connection: hand it straight back
    }
}

/// R-085: re-post the recv a spent pipeline budget suppressed, once the
/// queue has drained far enough. In-cell.
pub(crate) fn http_resume_reading(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, tid: u64) {
    let resume = match net.transports.get_mut(&tid).map(|e| &mut e.proto) {
        Some(ProtoKind::Http(conn)) if conn.pipeline_paused && conn.pipeline_drained() => {
            conn.pipeline_paused = false;
            true
        }
        _ => false,
    };
    if resume {
        post_recv(py, net, backend, tid);
    }
}

fn on_send_done(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    tid: u64,
    op: OpId,
    bytes: u32,
    os_error: u32,
) {
    let Some(entry) = net.transports.get_mut(&tid) else { return };
    if entry.send_op == Some(op) {
        entry.send_op = None;
    }
    if entry.conn_lost {
        return;
    }
    if os_error != 0 && !is_cancelled_error(os_error) {
        teardown_with(py, net, backend, tid, Some(os_error));
        return;
    }
    net.stats_bytes_tx += bytes as u64;
    // Pop consumed buffers into the graveyard (Py refs drop out-of-cell).
    let mut consumed = bytes as usize;
    entry.queued_bytes -= consumed.min(entry.queued_bytes);
    while consumed > 0 {
        let Some(front) = entry.wq.front_mut() else { break };
        let rem = front.remaining();
        if consumed >= rem {
            consumed -= rem;
            let buf = entry.wq.pop_front().unwrap();
            net.graveyard_bufs.push(buf);
        } else {
            front.advance(consumed);
            consumed = 0;
        }
    }
    // R-084: release an ASGI producer parked on the high-water mark --
    // counting staged plaintext, exactly as write_pressure does, so a
    // waiter is not woken while the TLS staging buffer is still full.
    if let ProtoKind::Http(conn) = &mut entry.proto {
        let staged = conn.tls.as_ref().map(|t| t.staged.len()).unwrap_or(0);
        if entry.queued_bytes + staged <= entry.low_water {
            if let Some(fut) = conn.drain_waiter.take() {
                net.events.push(NetEvent::HttpDrained { fut });
            }
        }
    }
    if entry.proto_paused && entry.queued_bytes <= entry.low_water {
        entry.proto_paused = false;
        if let ProtoKind::Py(p) = &entry.proto {
            let resume_writing = p.resume_writing.clone_ref(py);
            net.events.push(NetEvent::ResumeWriting { resume_writing });
        }
    }
    let entry = net.transports.get_mut(&tid).unwrap();
    if !entry.wq.is_empty() {
        flush_pending(py, net, backend, tid);
    } else {
        maybe_finish_shutdown(py, net, backend, tid);
    }
}

fn on_accept_done(net: &mut NetState, backend: Backend<'_>, lid: u64, op: OpId, os_error: u32) {
    let Some(listener) = net.listeners.get_mut(&lid) else {
        // Listener closed with the op in flight: reap the socket if any.
        if os_error == 0 {
            if let Ok(s) = backend.take_accept_socket(op) {
                netsys::close(s);
            }
        }
        return;
    };
    listener.accept_ops.retain(|&o| o != op);
    let closing = listener.closing;
    if os_error != 0 {
        if !closing {
            if !is_cancelled_error(os_error) {
                net.events.push(NetEvent::AcceptError { err: os_error });
            }
            // Re-arm even for an ABORTED completion: this op has left the
            // pool either way, and if it was the last one the listener
            // would otherwise stop accepting for good.
            post_accepts(net, backend, lid);
        }
        return;
    }
    if let Ok(sock) = backend.take_accept_socket(op) {
        net.stats_conns_accepted += 1;
        net.events.push(NetEvent::Accepted { lid, sock });
    }
    post_accepts(net, backend, lid);
}

// --------------------------------------------------------------------- //
// phase 2 — GIL held, outside the cell                                  //
// --------------------------------------------------------------------- //

/// Dispatch translated events. Fatal (KeyboardInterrupt/SystemExit) errors
/// propagate; protocol exceptions route to the loop's exception handler.
pub(crate) fn dispatch_events(
    py: Python<'_>,
    slf: &Bound<'_, CoreLoop>,
    events: Vec<NetEvent>,
) -> PyResult<()> {
    let core = slf.get();
    for event in events {
        match event {
            NetEvent::Data { tid, data_received, payload } => {
                let res = data_received.call1(py, (payload,));
                fatal_protocol_error(py, core, tid, res)?;
            }
            NetEvent::BufDataOwned { tid, get_buffer, buffer_updated, data } => {
                let res = copy_into_app_buffer(py, &get_buffer, &buffer_updated, data.as_ptr(), data.len());
                fatal_protocol_error(py, core, tid, res)?;
            }
            NetEvent::BufData { tid, get_buffer, buffer_updated, slot, len } => {
                let res = fill_app_buffer(py, core, &get_buffer, &buffer_updated, slot, len);
                core.with_net(|net, _| {
                    net.buffers.release(slot);
                })?;
                fatal_protocol_error(py, core, tid, res)?;
            }
            NetEvent::Eof { eof_received, transport } => match eof_received.call0(py) {
                Ok(keep_open) => {
                    if !keep_open.is_truthy(py).unwrap_or(false) {
                        transport.bind(py).get().close(py)?;
                    }
                }
                Err(e) => {
                    // Fatal, as it is in the stdlib. The receive side has
                    // reached EOF and will never be posted again, so a
                    // transport left open here is half-open forever unless
                    // the application happens to close it -- and it has
                    // just told us it cannot handle the event.
                    core.guard_protocol_call::<Py<PyAny>>(py, Err(e))?;
                    transport.bind(py).get().close(py)?;
                }
            },
            NetEvent::ConnLost { connection_lost, err } => {
                let exc_obj = match err {
                    Some(code) => os_err(py, code),
                    None => py.None(),
                };
                core.guard_protocol_call(py, connection_lost.call1(py, (exc_obj,)))?;
            }
            NetEvent::DgramData { datagram_received, payload, addr } => {
                let addr_obj = addr_tuple(py, addr).unwrap_or_else(|| py.None());
                core.guard_protocol_call(py, datagram_received.call1(py, (payload, addr_obj)))?;
            }
            NetEvent::DgramError { error_received, err } => {
                let exc_obj = os_err(py, err);
                core.guard_protocol_call(py, error_received.call1(py, (exc_obj,)))?;
            }
            NetEvent::DgramLost { connection_lost, err } => {
                let exc_obj = match err {
                    Some(code) => os_err(py, code),
                    None => py.None(),
                };
                core.guard_protocol_call(py, connection_lost.call1(py, (exc_obj,)))?;
            }
            NetEvent::ResumeWriting { resume_writing } => {
                core.guard_protocol_call(py, resume_writing.call0(py))?;
            }
            NetEvent::Accepted { lid, sock } => {
                enum Action {
                    Factory(Py<PyAny>),
                    Http {
                        app: Py<PyAny>,
                        pyloop: Py<PyAny>,
                        state: Py<PyAny>,
                        limits: Limits,
                        eager: bool,
                        tuning: HttpTuning,
                        tls: Option<Py<PyAny>>,
                    },
                }
                let action = core.with_net(|net, _| {
                    net.listeners.get(&lid).map(|l| match &l.kind {
                        ListenerKind::Factory(f) => Action::Factory(f.clone_ref(py)),
                        ListenerKind::Http { app, pyloop, state, limits, eager, tuning, tls } => {
                            Action::Http {
                                app: app.clone_ref(py),
                                pyloop: pyloop.clone_ref(py),
                                state: state.clone_ref(py),
                                limits: *limits,
                                eager: *eager,
                                tuning: *tuning,
                                tls: tls.as_ref().map(|t| t.clone_ref(py)),
                            }
                        }
                    })
                })?;
                match action {
                    None => netsys::close(sock),
                    Some(Action::Factory(factory)) => match factory.call0(py) {
                        Ok(protocol) => {
                            if let Err(e) = wire_stream(py, slf, sock, protocol.into_bound(py)) {
                                core.guard_protocol_call::<Py<PyAny>>(py, Err(e))?;
                            }
                        }
                        Err(e) => {
                            netsys::close(sock);
                            core.guard_protocol_call::<Py<PyAny>>(py, Err(e))?;
                        }
                    },
                    Some(Action::Http { app, pyloop, state, limits, eager, tuning, tls }) => {
                        if let Err(e) =
                            wire_http(py, slf, sock, app, pyloop, state, limits, eager, tuning, tls)
                        {
                            // wire_http leaves the descriptor to its caller
                            // on every failure (see its ownership note), so
                            // this branch owns the cleanup -- as the Factory
                            // branch above already does. Making wire_http
                            // stop closing without adding this was my own
                            // regression: it turned a double close into a
                            // leak of every connection whose registration or
                            // TLS setup failed.
                            core.with_net(|net, reactor| {
                                let _ = net;
                                reactor.backend_mut().detach_socket(sock);
                            })?;
                            netsys::close(sock);
                            core.guard_protocol_call::<Py<PyAny>>(py, Err(e))?;
                        }
                    }
                }
            }
            NetEvent::HttpPump { tid } => {
                crate::http::pump_requests(py, slf, tid)?;
            }
            NetEvent::HttpDisconnect { fut, ws } => {
                let fut = fut.bind(py);
                let done: bool = fut.call_method0("done").and_then(|v| v.extract()).unwrap_or(true);
                if !done {
                    let msg = if ws {
                        crate::http::ws_message_dict(py, crate::http::WsMsg::Disconnect(1006))?
                    } else {
                        crate::http::disconnect_message(py)?
                    };
                    let _ = fut.call_method1("set_result", (msg,));
                }
            }
            NetEvent::HttpDrained { fut } => {
                let fut = fut.bind(py);
                let done: bool = fut.call_method0("done").and_then(|v| v.extract()).unwrap_or(true);
                if !done {
                    let _ = fut.call_method1("set_result", (py.None(),));
                }
            }
            NetEvent::WsWake { tid, fut } => {
                // Deliver ONE queued WS event to the waiter (R-087).
                //
                // This is the STEADY-STATE path -- an app already parked in
                // receive() is woken here, not through HttpReceive.__call__
                // -- so the inbox budget must be decremented here too. It
                // was not, so inbox_bytes only ever grew: after ~4 MiB of
                // cumulative traffic ws_ingest paused reads permanently on
                // a connection whose application had consumed every single
                // message.
                let msg = core.with_net(|net, _| {
                    let m =
                        net.http_conn_mut(tid).and_then(|c| c.ws.as_mut()).and_then(|w| w.inbox.pop_front());
                    if let Some(m) = m.as_ref() {
                        if let Some(w) = net.http_conn_mut(tid).and_then(|c| c.ws.as_mut()) {
                            w.inbox_bytes = w.inbox_bytes.saturating_sub(m.byte_len());
                        }
                    }
                    m
                })?;
                if msg.is_some() {
                    core.with_net(|net, reactor| {
                        crate::http::ws_resume_reading(py, net, reactor.backend_mut(), tid);
                    })?;
                }
                let fut = fut.bind(py);
                let done: bool = fut.call_method0("done").and_then(|v| v.extract()).unwrap_or(true);
                match (done, msg) {
                    (false, Some(m)) => {
                        let d = crate::http::ws_message_dict(py, m)?;
                        let _ = fut.call_method1("set_result", (d,));
                    }
                    (false, None) => {
                        // Spurious wake: park the waiter again.
                        let stored = fut.clone().unbind();
                        core.with_net(|net, _| {
                            if let Some(c) = net.http_conn_mut(tid) {
                                c.recv_waiter = Some(stored);
                            }
                        })?;
                    }
                    (true, Some(m)) => {
                        // Waiter was cancelled: keep the event for the next
                        // receive() -- and put its bytes BACK on the inbox
                        // budget, which was debited when it was popped
                        // above. Without this, repeated cancellable
                        // receives drift the accounting down until real
                        // traffic can exceed WS_MAX_INBOX with the read
                        // pause never engaging.
                        core.with_net(|net, _| {
                            if let Some(w) = net.http_conn_mut(tid).and_then(|c| c.ws.as_mut()) {
                                w.inbox_bytes = w.inbox_bytes.saturating_add(m.byte_len());
                                w.inbox.push_front(m);
                            }
                        })?;
                    }
                    (true, None) => {}
                }
            }
            NetEvent::TlsData { tid, data } => {
                crate::http::tls_ingest(py, slf, tid, &data)?;
            }
            NetEvent::TlsFlush { tid } => {
                crate::http::tls_flush_conn(py, slf, tid)?;
            }
            NetEvent::AcceptError { err } => {
                core.report_net_error(py, "Accept failed", os_err(py, err));
            }
            NetEvent::ConnectDone { fut, sock, err } => {
                let fut = fut.bind(py);
                let cancelled: bool = fut.call_method0("cancelled").and_then(|v| v.extract()).unwrap_or(true);
                if err != 0 {
                    netsys::close(sock);
                    if !cancelled {
                        let _ = fut.call_method1("set_exception", (os_err(py, err),));
                    }
                } else if cancelled {
                    netsys::close(sock);
                } else {
                    let _ = fut.call_method1("set_result", (sock as u64,));
                }
            }
            NetEvent::PipeReadDone { fut, buf, bytes, err } => {
                let fut = fut.bind(py);
                let cancelled: bool = fut.call_method0("cancelled").and_then(|v| v.extract()).unwrap_or(true);
                if cancelled {
                    // nothing to deliver; the transport already moved on
                } else if err != 0 && err != ERROR_BROKEN_PIPE {
                    let _ = fut.call_method1("set_exception", (os_err(py, err),));
                } else {
                    // ERROR_BROKEN_PIPE (the writer closed) is EOF, same
                    // convention as a genuine 0-byte successful read —
                    // matches stdlib's IocpProactor.recv() precisely.
                    let data = PyBytes::new(py, &buf[..bytes as usize]);
                    let _ = fut.call_method1("set_result", (data,));
                }
            }
            NetEvent::PipeWriteDone { fut, bytes, err } => {
                let fut = fut.bind(py);
                let cancelled: bool = fut.call_method0("cancelled").and_then(|v| v.extract()).unwrap_or(true);
                if cancelled {
                    // nothing to deliver
                } else if err != 0 {
                    let _ = fut.call_method1("set_exception", (os_err(py, err),));
                } else {
                    let _ = fut.call_method1("set_result", (bytes,));
                }
            }
        }
    }
    Ok(())
}

/// Win32 ERROR_BROKEN_PIPE: ReadFile on a pipe whose write end closed can
/// fail with this instead of returning a 0-byte success — stdlib's own
/// proactor treats it identically to EOF (R-051). `pub(crate)` since
/// coreloop.rs's `pipe_read` needs the same constant to apply the
/// identical translation on the *synchronous*-failure path (ReadFile
/// returning FALSE immediately never queues a completion, so it can't
/// be handled here in the poll-dispatch path at all).
pub(crate) const ERROR_BROKEN_PIPE: u32 = 109;

/// BufferedProtocol receive: get_buffer -> memcpy -> buffer_updated.
fn fill_app_buffer(
    py: Python<'_>,
    core: &CoreLoop,
    get_buffer: &Py<PyAny>,
    buffer_updated: &Py<PyAny>,
    slot: SlotId,
    len: usize,
) -> PyResult<Py<PyAny>> {
    let src = core.with_net(|net, _| net.buffers.slot_ptr(slot))?;
    copy_into_app_buffer(py, get_buffer, buffer_updated, src, len)
}

/// The copy loop itself, over any source. Split out so held bytes (which
/// have no pool slot) reach the protocol by exactly the same path as a
/// fresh read.
///
/// SAFETY: `src` must be valid for `len` bytes and must stay valid across
/// the `get_buffer`/`buffer_updated` calls -- either a live pool slot the
/// caller still owns, or a `Vec` the caller keeps alive.
fn copy_into_app_buffer(
    py: Python<'_>,
    get_buffer: &Py<PyAny>,
    buffer_updated: &Py<PyAny>,
    src: *const u8,
    len: usize,
) -> PyResult<Py<PyAny>> {
    let mut copied = 0usize;
    while copied < len {
        let buf_obj = get_buffer.call1(py, (len - copied,))?;
        let mut view: ffi::Py_buffer = unsafe { std::mem::zeroed() };
        let rc = unsafe { ffi::PyObject_GetBuffer(buf_obj.as_ptr(), &mut view, ffi::PyBUF_WRITABLE) };
        if rc != 0 {
            return Err(PyErr::fetch(py));
        }
        let chunk = (view.len as usize).min(len - copied);
        if chunk == 0 {
            unsafe { ffi::PyBuffer_Release(&mut view) };
            return Err(PyRuntimeError::new_err("get_buffer() returned a zero-sized buffer"));
        }
        unsafe {
            std::ptr::copy_nonoverlapping(src.add(copied), view.buf.cast::<u8>(), chunk);
            ffi::PyBuffer_Release(&mut view);
        }
        copied += chunk;
        buffer_updated.call1(py, (chunk,))?;
    }
    Ok(py.None())
}

/// Cache protocol callbacks (R-054 bound-method cache). Phase-2 only.
fn cache_proto(py: Python<'_>, protocol: &Bound<'_, PyAny>) -> PyResult<ProtoRefs> {
    let _ = py;
    let is_buffered = protocol.hasattr("get_buffer")? && protocol.hasattr("buffer_updated")?;
    Ok(ProtoRefs {
        protocol: protocol.clone().unbind(),
        data_received: if is_buffered { None } else { Some(protocol.getattr("data_received")?.unbind()) },
        get_buffer: if is_buffered { Some(protocol.getattr("get_buffer")?.unbind()) } else { None },
        buffer_updated: if is_buffered { Some(protocol.getattr("buffer_updated")?.unbind()) } else { None },
        eof_received: protocol.getattr("eof_received")?.unbind(),
        connection_lost: protocol.getattr("connection_lost")?.unbind(),
        pause_writing: protocol.getattr("pause_writing")?.unbind(),
        resume_writing: protocol.getattr("resume_writing")?.unbind(),
    })
}

/// The Python form of a socket address: `(host, port)` for IPv4 and
/// `(host, port, flowinfo, scope_id)` for IPv6, exactly as every socket
/// API in the standard library represents them.
///
/// Flattening IPv6 to two elements loses the scope id, and for a
/// link-local peer that is not cosmetic: the address a callback hands the
/// application can no longer be passed back to `sendto()`, because
/// without the interface scope the reply is unroutable or leaves through
/// the wrong interface.
/// The Python form of a socket address of any family.
///
/// AF_UNIX is a plain path string (or `""` for an unnamed socket), which
/// is what the socket module and asyncio both use. Before this, an
/// AF_UNIX address failed to parse as an Internet one and was dropped, so
/// `get_extra_info("peername"/"sockname")` returned None on a live Unix
/// connection.
fn any_addr_tuple(py: Python<'_>, addr: Option<netsys::Addr>) -> Option<Py<PyAny>> {
    match addr {
        Some(netsys::Addr::Inet(a)) => addr_tuple(py, Some(a)),
        Some(netsys::Addr::Unix(path)) => {
            let s = String::from_utf8_lossy(&path).into_owned();
            Some(s.into_pyobject(py).ok()?.into_any().unbind())
        }
        None => None,
    }
}

fn addr_tuple(py: Python<'_>, addr: Option<std::net::SocketAddr>) -> Option<Py<PyAny>> {
    addr.map(|a| match a {
        std::net::SocketAddr::V4(v4) => {
            (v4.ip().to_string(), v4.port()).into_pyobject(py).unwrap().into_any().unbind()
        }
        std::net::SocketAddr::V6(v6) => (v6.ip().to_string(), v6.port(), v6.flowinfo(), v6.scope_id())
            .into_pyobject(py)
            .unwrap()
            .into_any()
            .unbind(),
    })
}

/// Wire an accepted/connected socket into a live transport. Phase-2.
pub(crate) fn wire_stream(
    py: Python<'_>,
    slf: &Bound<'_, CoreLoop>,
    sock: RawSocket,
    protocol: Bound<'_, PyAny>,
) -> PyResult<Py<Transport>> {
    let core = slf.get();
    // Everything up to the transport insert is transactional: on any
    // failure the socket has no owner (the caller detached it, or it came
    // back from tcp_connect), so it must be released here or it leaks.
    // Detach-then-close, never bare close: tcp_connect registered it, and
    // closing a registered socket leaves the backend listing a handle
    // value the OS will reissue, so the next socket to reuse it looks
    // already-associated and its completions are never queued (ADR-25).
    let discard = |core: &CoreLoop| {
        let _ = core.with_net(|_net, reactor| reactor.backend_mut().detach_socket(sock));
        netsys::close(sock);
    };
    let proto = match cache_proto(py, &protocol) {
        Ok(p) => p,
        Err(e) => {
            discard(core);
            return Err(e);
        }
    };
    let _ = netsys::set_nodelay(sock, true); // R-038
    let peer = any_addr_tuple(py, netsys::peername_any(sock).ok());
    let name = any_addr_tuple(py, netsys::sockname_any(sock).ok());
    // A protocol without connection_made reaches here with the descriptor
    // still unowned; `?` alone would have dropped it on the floor.
    let connection_made = match protocol.getattr("connection_made") {
        Ok(cb) => cb,
        Err(e) => {
            discard(core);
            return Err(e);
        }
    };

    let (high, low) = core.water_marks();
    let reg = core.with_net(|_net, reactor| reactor.backend_mut().register_socket(sock))?;
    if let Err(e) = reg {
        // register_socket failed, so it is NOT associated: a bare close is
        // right here and a detach would be meaningless.
        netsys::close(sock);
        return Err(e.into());
    }
    let tid = core.with_net(|net, _| net.next_id())?;
    let transport = Py::new(py, Transport { core: slf.clone().unbind(), tid })?;
    core.with_net(|net, _| {
        net.transports.insert(
            tid,
            TransportEntry {
                socket: sock,
                proto: ProtoKind::Py(proto),
                pyobj: Some(transport.clone_ref(py)),
                recv_slot: None,
                recv_op: None,
                send_op: None,
                wq: VecDeque::new(),
                queued_bytes: 0,
                high_water: high,
                low_water: low,
                reading_paused: false,
                proto_paused: false,
                closing: false,
                conn_lost: false,
                eof_wanted: false,
                eof_sent: false,
                flush_scheduled: false,
                paused_recv: None,
                extra_sock: None,
                peername: peer,
                sockname: name,
            },
        );
    })?;
    // connection_made BEFORE the first recv is posted: no data callback can
    // possibly precede it.
    let made = connection_made.call1((transport.clone_ref(py),));
    core.guard_protocol_call(py, made.map(|b| b.unbind()))?;
    core.with_net(|net, reactor| post_recv(py, net, reactor.backend_mut(), tid))?;
    core.drain_graveyards(py)?;
    Ok(transport)
}

/// Wire an accepted socket into a native HTTP connection (M2). Phase-2.
/// No Python protocol, no Transport pyobj — the connection lives entirely
/// inside the state cell until requests are dispatched.
#[allow(clippy::too_many_arguments)]
pub(crate) fn wire_http(
    py: Python<'_>,
    slf: &Bound<'_, CoreLoop>,
    sock: RawSocket,
    app: Py<PyAny>,
    pyloop: Py<PyAny>,
    state: Py<PyAny>,
    limits: Limits,
    eager: bool,
    tuning: HttpTuning,
    tls: Option<Py<PyAny>>,
) -> PyResult<()> {
    let core = slf.get();
    let _ = netsys::set_nodelay(sock, true); // R-038
    let peer = any_addr_tuple(py, netsys::peername_any(sock).ok());
    let name = any_addr_tuple(py, netsys::sockname_any(sock).ok());
    let (high, low) = core.water_marks();
    // OWNERSHIP: the CALLER owns `sock` until this returns Ok. Closing it
    // here as well as in the caller's error path meant a failed adoption
    // closed the same descriptor number twice -- and if another thread had
    // been handed that number meanwhile, it closed an unrelated socket.
    let reg = core.with_net(|_net, reactor| reactor.backend_mut().register_socket(sock))?;
    if let Err(e) = reg {
        return Err(e.into());
    }
    let tls_state = match &tls {
        Some(ctx) => Some(Box::new(crate::http::tls_wrap(py, ctx)?)),
        None => None,
    };
    core.with_net(|net, reactor| {
        let tid = net.next_id();
        let mut conn =
            HttpConn::new(app, pyloop, state, limits, eager, tuning.head_timeout_ns, tuning.idle_timeout_ns);
        conn.tls = tls_state;
        net.transports.insert(
            tid,
            TransportEntry {
                socket: sock,
                proto: ProtoKind::Http(Box::new(conn)),
                pyobj: None,
                recv_slot: None,
                recv_op: None,
                send_op: None,
                wq: VecDeque::new(),
                queued_bytes: 0,
                high_water: high,
                low_water: low,
                reading_paused: false,
                proto_paused: false,
                closing: false,
                conn_lost: false,
                eof_wanted: false,
                eof_sent: false,
                flush_scheduled: false,
                paused_recv: None,
                extra_sock: None,
                peername: peer,
                sockname: name,
            },
        );
        post_recv(py, net, reactor.backend_mut(), tid);
    })?;
    Ok(())
}

// --------------------------------------------------------------------- //
// the Transport pyclass                                                 //
// --------------------------------------------------------------------- //

/// asyncio.Transport implementation backed by the native reactor.
#[pyclass(frozen, module = "cadeloop._core")]
pub struct Transport {
    pub(crate) core: Py<CoreLoop>,
    pub(crate) tid: u64,
}

impl Transport {
    fn core_ref<'a>(&'a self, py: Python<'a>) -> &'a CoreLoop {
        self.core.bind(py).get()
    }

    /// `get_extra_info("socket")`.
    ///
    /// Protocols and libraries reach for this through the standard
    /// transport API to read the address family, inspect socket options,
    /// or set one such as keepalive. Returning `None` for a live
    /// connection made them either fail or silently skip that setup.
    ///
    /// The object owns a `dup()` rather than the engine's descriptor,
    /// because two Python-visible owners of one descriptor means a double
    /// close, and by the second one the OS may have handed that number to
    /// an unrelated connection (ADR-25's hazard, reached from the other
    /// side). But a duplicate is still an *owner* of the connection: if
    /// the application keeps the object, the peer gets no EOF when the
    /// transport closes. So teardown closes it explicitly -- see
    /// `graveyard_sockets` -- which makes the transport's close final
    /// whether or not anyone kept a reference.
    ///
    /// Built on first ask (nothing pays for it otherwise) and kept on the
    /// entry, so repeated calls return the same object rather than
    /// leaking one duplicate per call.
    fn extra_socket(&self, py: Python<'_>, core: &CoreLoop) -> PyResult<Option<Py<PyAny>>> {
        let (cached, raw) = core.with_net(|net, _| match net.transports.get(&self.tid) {
            Some(e) => (e.extra_sock.as_ref().map(|s| s.clone_ref(py)), Some(e.socket)),
            None => (None, None),
        })?;
        if cached.is_some() {
            return Ok(cached);
        }
        let Some(raw) = raw else { return Ok(None) };
        let obj = build_transport_socket(py, raw)?;
        // Only ever set from None (the cached branch above returns
        // first), so nothing is dropped inside the cell -- ADR-5.
        core.with_net(|net, _| {
            if let Some(e) = net.transports.get_mut(&self.tid) {
                e.extra_sock = Some(obj.clone_ref(py));
            }
        })?;
        Ok(Some(obj))
    }
}

/// Report an exception out of a receive callback and close the
/// connection it arrived on.
///
/// The receive for the next chunk has already been posted by the time the
/// callback runs, so merely reporting the exception (which is what the
/// generic `guard_protocol_call` does) left the connection open and kept
/// feeding bytes to a protocol whose state may be inconsistent. The
/// stdlib's socket transport treats this as fatal and closes; so does
/// this, and the Windows pipe transport already did.
fn fatal_protocol_error<T>(py: Python<'_>, core: &CoreLoop, tid: u64, res: PyResult<T>) -> PyResult<()> {
    if res.is_ok() {
        return Ok(());
    }
    core.guard_protocol_call(py, res)?;
    core.with_net(|net, reactor| {
        teardown_with(py, net, reactor.backend_mut(), tid, None);
    })?;
    core.drain_graveyards(py)
}

/// A plain `socket.socket` over a duplicate of `raw`.
///
/// Not wrapped in `asyncio.trsock.TransportSocket`: that wrapper's whole
/// job is to stop callers closing a descriptor the transport owns, and
/// here the descriptor is a duplicate the *engine* must be able to close
/// at teardown. A real socket object is also the more useful thing to
/// hand back -- `.family`, `.getsockopt`, `.setsockopt` and the rest are
/// what libraries actually reach for, and they work on it directly.
fn build_transport_socket(py: Python<'_>, raw: RawSocket) -> PyResult<Py<PyAny>> {
    let socket_mod = py.import("socket")?;
    // socket.dup(), not os.dup(): on Windows a SOCKET is not a file
    // descriptor and only this one does the right thing.
    let dup = socket_mod.call_method1(intern!(py, "dup"), (raw as u64,))?;
    let kwargs = PyDict::new(py);
    kwargs.set_item(intern!(py, "fileno"), dup)?;
    Ok(socket_mod.getattr(intern!(py, "socket"))?.call((), Some(&kwargs))?.unbind())
}

#[pymethods]
impl Transport {
    // ---- write path ---------------------------------------------------

    fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let core = self.core_ref(py);
        // R-074 retention: exact bytes -> zero-copy retain; any other
        // buffer exporter -> copy now.
        let buf: WriteBuf = if let Ok(b) = data.cast::<PyBytes>() {
            let len = b.as_bytes().len();
            if len == 0 {
                return Ok(());
            }
            WriteBuf::Bytes { ptr: b.as_bytes().as_ptr(), len, off: 0, _keep: b.clone().into_any().unbind() }
        } else {
            let mut view: ffi::Py_buffer = unsafe { std::mem::zeroed() };
            let rc = unsafe { ffi::PyObject_GetBuffer(data.as_ptr(), &mut view, ffi::PyBUF_SIMPLE) };
            if rc != 0 {
                return Err(PyErr::fetch(py));
            }
            let len = view.len as usize;
            if len == 0 {
                unsafe { ffi::PyBuffer_Release(&mut view) };
                return Ok(());
            }
            let mut owned = vec![0u8; len];
            unsafe {
                std::ptr::copy_nonoverlapping(view.buf.cast::<u8>(), owned.as_mut_ptr(), len);
                ffi::PyBuffer_Release(&mut view);
            }
            WriteBuf::Owned { data: owned, off: 0 }
        };

        let tid = self.tid;
        let mut pause: Option<Py<PyAny>> = None;
        let mut dirty = false;
        let mut eof_misuse = false;
        core.with_net(|net, reactor| {
            let Some(entry) = net.transports.get_mut(&tid) else {
                net.graveyard_bufs.push(buf);
                dirty = true;
                return; // write after connection_lost: silently dropped
            };
            if entry.eof_wanted && !entry.closing && !entry.conn_lost {
                // Half-close MISUSE, not a dead connection: the asyncio
                // write-transport contract raises here. Silently
                // succeeding let protocol code believe output was queued
                // when it can never reach the peer -- undetected
                // truncation.
                net.graveyard_bufs.push(buf);
                dirty = true;
                eof_misuse = true;
                return;
            }
            if entry.closing || entry.conn_lost {
                net.graveyard_bufs.push(buf);
                dirty = true;
                return;
            }
            entry.queued_bytes += buf.remaining();
            entry.wq.push_back(buf);
            let no_send = entry.send_op.is_none();
            let big = entry.queued_bytes >= CORK_FLUSH_BYTES;
            let scheduled = entry.flush_scheduled;
            if no_send {
                if big {
                    flush_pending(py, net, reactor.backend_mut(), tid); // R-035 early flush
                } else if !scheduled {
                    // R-035 corking: coalesce writes within the tick.
                    if let Some(e) = net.transports.get_mut(&tid) {
                        e.flush_scheduled = true;
                    }
                    net.flush_list.push(tid);
                }
            }
            if let Some(entry) = net.transports.get_mut(&tid) {
                if entry.queued_bytes > entry.high_water && !entry.proto_paused {
                    entry.proto_paused = true;
                    if let ProtoKind::Py(p) = &entry.proto {
                        pause = Some(p.pause_writing.clone_ref(py));
                    }
                }
            }
            dirty |= !net.graveyard_bufs.is_empty() || !net.graveyard_entries.is_empty();
        })?;
        if dirty {
            // Rare paths only (dropped write, >=64KiB flush that tore down).
            core.drain_graveyards(py)?;
        }
        if eof_misuse {
            return Err(PyRuntimeError::new_err("Cannot call write() after write_eof()"));
        }
        if let Some(pause_writing) = pause {
            core.guard_protocol_call(py, pause_writing.call0(py))?;
        }
        Ok(())
    }

    fn writelines(&self, py: Python<'_>, list_of_data: Bound<'_, PyAny>) -> PyResult<()> {
        for item in list_of_data.try_iter()? {
            self.write(py, item?)?;
        }
        Ok(())
    }

    fn write_eof(&self, py: Python<'_>) -> PyResult<()> {
        let core = self.core_ref(py);
        let tid = self.tid;
        core.with_net(|net, reactor| {
            if let Some(entry) = net.transports.get_mut(&tid) {
                entry.eof_wanted = true;
            }
            maybe_finish_shutdown(py, net, reactor.backend_mut(), tid);
        })?;
        core.drain_graveyards(py)
    }

    fn can_write_eof(&self) -> bool {
        true
    }

    fn get_write_buffer_size(&self, py: Python<'_>) -> PyResult<usize> {
        let core = self.core_ref(py);
        core.with_net(|net, _| net.transports.get(&self.tid).map_or(0, |e| e.queued_bytes))
    }

    fn get_write_buffer_limits(&self, py: Python<'_>) -> PyResult<(usize, usize)> {
        let core = self.core_ref(py);
        core.with_net(|net, _| net.transports.get(&self.tid).map_or((0, 0), |e| (e.low_water, e.high_water)))
    }

    #[pyo3(signature = (high=None, low=None))]
    fn set_write_buffer_limits(
        &self,
        py: Python<'_>,
        high: Option<usize>,
        low: Option<usize>,
    ) -> PyResult<()> {
        let core = self.core_ref(py);
        // asyncio's rule: high defaults to 4 * low when only `low` is
        // supplied (not a fixed 64 KiB, which rejected low > 64 KiB
        // outright and gave a different pause threshold from every other
        // loop for smaller values).
        let (high, low) = match (high, low) {
            (Some(h), Some(l)) => (h, l),
            (Some(h), None) => (h, h / 4),
            (None, Some(l)) => (l * 4, l),
            (None, None) => (64 * 1024, 16 * 1024),
        };
        if low > high {
            return Err(pyo3::exceptions::PyValueError::new_err("high must be >= low must be >= 0"));
        }
        // Applying the limits is not enough: the queue already has a
        // depth, and the pause/resume decision has to be re-taken against
        // the new marks. Lowering `high` under an existing backlog left
        // the protocol unpaused until its next write; raising the limits
        // on a paused transport could leave it paused indefinitely if no
        // further write completion arrived.
        let action = core.with_net(|net, _| {
            let e = net.transports.get_mut(&self.tid)?;
            e.high_water = high;
            e.low_water = low;
            if !e.proto_paused && e.queued_bytes > e.high_water {
                e.proto_paused = true;
                if let ProtoKind::Py(p) = &e.proto {
                    return Some((true, p.pause_writing.clone_ref(py)));
                }
            } else if e.proto_paused && e.queued_bytes <= e.low_water {
                e.proto_paused = false;
                if let ProtoKind::Py(p) = &e.proto {
                    return Some((false, p.resume_writing.clone_ref(py)));
                }
            }
            None
        })?;
        if let Some((_pausing, cb)) = action {
            core.guard_protocol_call(py, cb.call0(py))?;
        }
        Ok(())
    }

    // ---- read path -----------------------------------------------------

    fn pause_reading(&self, py: Python<'_>) -> PyResult<()> {
        let core = self.core_ref(py);
        let tid = self.tid;
        let _ = tid;
        core.with_net(|net, _reactor| {
            if let Some(entry) = net.transports.get_mut(&tid) {
                // The in-flight recv is left to complete -- cancelling
                // races the completion and the slot reuse, and on Windows
                // the bytes have already left the kernel buffer, so they
                // would be lost. But its data is HELD, not delivered: a
                // protocol pauses to bound its own memory, and handing it
                // one more full buffer is precisely what it asked us not
                // to do. resume_reading delivers it, ahead of the next
                // read so the stream stays in order.
                entry.reading_paused = true;
            }
        })
    }

    fn resume_reading(&self, py: Python<'_>) -> PyResult<()> {
        let core = self.core_ref(py);
        let tid = self.tid;
        core.with_net(|net, reactor| {
            if let Some(entry) = net.transports.get_mut(&tid) {
                if entry.reading_paused {
                    entry.reading_paused = false;
                    // Anything that completed while paused goes out first,
                    // ahead of whatever the fresh recv brings, or the
                    // stream would be reordered.
                    flush_paused_recv(py, net, tid);
                    let still_idle = net.transports.get(&tid).is_some_and(|e| e.recv_op.is_none());
                    if still_idle {
                        post_recv(py, net, reactor.backend_mut(), tid);
                    }
                }
            }
        })
    }

    fn is_reading(&self, py: Python<'_>) -> PyResult<bool> {
        let core = self.core_ref(py);
        core.with_net(|net, _| {
            net.transports.get(&self.tid).is_some_and(|e| !e.reading_paused && !e.conn_lost)
        })
    }

    // ---- lifecycle -----------------------------------------------------

    pub(crate) fn close(&self, py: Python<'_>) -> PyResult<()> {
        let core = self.core_ref(py);
        let tid = self.tid;
        core.with_net(|net, reactor| {
            let backend = reactor.backend_mut();
            let Some(entry) = net.transports.get_mut(&tid) else { return };
            if entry.closing || entry.conn_lost {
                return;
            }
            entry.closing = true;
            if let Some(op) = entry.recv_op.take() {
                // Keep the mapping (see pause_reading): the completion is
                // routed and discarded by the closing/conn_lost checks.
                let _ = backend.cancel(op);
            }
            maybe_finish_shutdown(py, net, backend, tid);
        })?;
        core.drain_graveyards(py)
    }

    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        let core = self.core_ref(py);
        let tid = self.tid;
        core.with_net(|net, reactor| teardown_with(py, net, reactor.backend_mut(), tid, None))?;
        core.drain_graveyards(py)
    }

    fn is_closing(&self, py: Python<'_>) -> PyResult<bool> {
        let core = self.core_ref(py);
        core.with_net(|net, _| net.transports.get(&self.tid).is_none_or(|e| e.closing || e.conn_lost))
    }

    // ---- protocol / introspection ---------------------------------------

    fn set_protocol(&self, py: Python<'_>, protocol: Bound<'_, PyAny>) -> PyResult<()> {
        let refs = cache_proto(py, &protocol)?;
        let core = self.core_ref(py);
        core.with_net(|net, _| {
            if let Some(entry) = net.transports.get_mut(&self.tid) {
                // Drops must happen out-of-cell — Py refs land in
                // graveyard_py, an HttpConn (unreachable here in practice:
                // no Transport pyobj exists for HTTP conns) in
                // graveyard_protos.
                let old = std::mem::replace(&mut entry.proto, ProtoKind::Py(refs));
                match old {
                    ProtoKind::Py(old) => {
                        net.graveyard_py.push(old.protocol);
                        net.graveyard_py.push(old.connection_lost);
                        net.graveyard_py.push(old.eof_received);
                        net.graveyard_py.push(old.pause_writing);
                        net.graveyard_py.push(old.resume_writing);
                        if let Some(m) = old.data_received {
                            net.graveyard_py.push(m);
                        }
                        if let Some(m) = old.get_buffer {
                            net.graveyard_py.push(m);
                        }
                        if let Some(m) = old.buffer_updated {
                            net.graveyard_py.push(m);
                        }
                    }
                    old @ ProtoKind::Http(_) => net.graveyard_protos.push(old),
                }
            } else {
                net.graveyard_py.push(refs.protocol);
            }
        })?;
        core.drain_graveyards(py)
    }

    fn get_protocol(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let core = self.core_ref(py);
        core.with_net(|net, _| match net.transports.get(&self.tid) {
            Some(TransportEntry { proto: ProtoKind::Py(p), .. }) => p.protocol.clone_ref(py),
            _ => py.None(),
        })
    }

    /// The transport's raw socket handle, BORROWED (the transport still
    /// owns and closes it). Powers loop.sendfile's native path (R-036).
    fn fileno(&self, py: Python<'_>) -> PyResult<u64> {
        let core = self.core_ref(py);
        core.with_net(|net, _| {
            net.transports
                .get(&self.tid)
                .map(|e| e.socket as u64)
                .ok_or_else(|| PyRuntimeError::new_err("transport is closed"))
        })?
    }

    #[pyo3(signature = (name, default=None))]
    fn get_extra_info(&self, py: Python<'_>, name: &str, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let core = self.core_ref(py);
        if name == "socket" {
            let out = self.extra_socket(py, core)?;
            return Ok(out.or(default).unwrap_or_else(|| py.None()));
        }
        let out = core.with_net(|net, _| {
            net.transports.get(&self.tid).and_then(|e| match name {
                "peername" => e.peername.as_ref().map(|p| p.clone_ref(py)),
                "sockname" => e.sockname.as_ref().map(|p| p.clone_ref(py)),
                _ => None,
            })
        })?;
        Ok(out.or(default).unwrap_or_else(|| py.None()))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let closing = self.is_closing(py)?;
        Ok(format!("<cadeloop.Transport tid={} closing={closing}>", self.tid))
    }
}
