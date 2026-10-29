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

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap as HashMap;

use cadeloop_core::backend::{
    io_len, is_cancelled_error, Completion, IoBackend, IoSlice, RawSocket, MAX_SEND_BYTES,
};
use cadeloop_core::buffers::{BufferPool, SizeClass, SlotId};
use cadeloop_core::http::{Limits, ParseError};
use cadeloop_core::netsys;
use cadeloop_core::opslab::OpId;
use pyo3::exceptions::{PyKeyboardInterrupt, PyOSError, PyRuntimeError, PySystemExit};
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict};

use crate::coreloop::CoreLoop;
use crate::handles::Handle;
use crate::http::HttpConn;

/// R-035: cork flush threshold.
const CORK_FLUSH_BYTES: usize = 64 * 1024;
/// Recv slot class: 64 KiB reads.
const RECV_CLASS: SizeClass = SizeClass::S64K;

/// The OS's message for `code` -- what `os.strerror()` (or, on Windows,
/// a socket.error) says -- without the " (os error N)" suffix that Rust's
/// `io::Error` display appends.
pub(crate) fn os_strerror(code: i32) -> String {
    let mut s = io::Error::from_raw_os_error(code).to_string();
    if let Some(i) = s.rfind(" (os error ") {
        s.truncate(i);
    }
    // CPython trims FormatMessage's trailing period and line break.
    #[cfg(windows)]
    s.truncate(s.trim_end_matches(|c: char| c == '.' || c.is_whitespace()).len());
    s
}

/// `OSError(code, msg)` built the way the socket module builds its own:
/// `errno` and `strerror` set, and the subclass (ConnectionRefusedError,
/// BrokenPipeError, ...) picked from the code by OSError itself.
///
/// PyO3's `From<io::Error>` keeps only the display string, so every
/// native failure used to arrive with `errno=None` and a Rust-style
/// "Connection refused (os error 111)" message -- and code that branches
/// on `exc.errno` (asyncio's own create_server skipping an address family
/// the host lacks, retry loops checking EADDRINUSE) never matched.
pub(crate) fn os_error_msg(code: i32, msg: String) -> PyErr {
    // The four-argument form sets `winerror`, from which OSError derives
    // errno -- what socket.error carries on Windows.
    #[cfg(windows)]
    {
        PyOSError::new_err((code, msg, Option::<i32>::None, code))
    }
    #[cfg(not(windows))]
    {
        PyOSError::new_err((code, msg))
    }
}

pub(crate) fn os_error(code: i32) -> PyErr {
    os_error_msg(code, os_strerror(code))
}

/// `io::Error` -> `OSError`, keeping the errno (see `os_error_msg`).
pub(crate) fn io_error(e: io::Error) -> PyErr {
    match e.raw_os_error() {
        Some(code) => os_error(code),
        None => e.into(),
    }
}

pub(crate) fn os_err(py: Python<'_>, code: u32) -> Py<PyAny> {
    os_error(code as i32).into_value(py).into_any()
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
                IoSlice { ptr: unsafe { ptr.add(*off) }, len: io_len(len - off) }
            }
            WriteBuf::Owned { data, off } => {
                IoSlice { ptr: unsafe { data.as_ptr().add(*off) }, len: io_len(data.len() - off) }
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
    /// The peer's EOF arrived while `reading_paused` was set: held like
    /// `paused_recv` (asyncio sees no EOF before resume_reading re-adds
    /// its reader), and delivered after it.
    paused_eof: bool,
    /// The duplicate behind `get_extra_info("socket")` of a transport
    /// that has no socket object of its own (see `Transport::trsock`),
    /// built on first ask. Closed, not merely dropped, at teardown -- see
    /// `graveyard_sockets`.
    pub extra_sock: Option<Py<PyAny>>,
    /// That duplicate's descriptor, listed in `fd_users` too: it is the
    /// number `get_extra_info("socket").fileno()` reports, so it is the
    /// one an add_reader() on "the transport's fd" names.
    extra_fd: Option<RawSocket>,
    /// The caller's own socket object, for a transport built from one
    /// (`sock=`). It owns the descriptor, as in asyncio: teardown closes
    /// it (through Python, once connection_lost has run) instead of
    /// closing the descriptor out from under it, and until then the
    /// caller's object keeps working.
    pub owner: Option<Py<PyAny>>,
    /// The asyncio-level Server whose listener accepted this connection:
    /// told `_detach()` once connection_lost has run, which is what
    /// `Server.wait_closed()` waits for.
    pub server: Option<Py<PyAny>>,
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
    /// The caller's listening socket object (`create_server(sock=...)`),
    /// which owns the descriptor and is closed through Python at
    /// teardown -- see `TransportEntry::owner`.
    pub owner: Option<Py<PyAny>>,
    /// The asyncio Server facade, handed to each accepted transport so it
    /// can count live connections (`listener_set_server`).
    pub server: Option<Py<PyAny>>,
    accept_ops: Vec<OpId>,
    target: usize,
    closing: bool,
    /// The pool ran dry and `post_accept` refused (transiently, e.g. a
    /// descriptor limit). Nothing is outstanding, so no completion can
    /// wake this listener again -- the next tick retries instead.
    starved: bool,
    /// Earliest loop time (ns) at which a starved listener is retried.
    retry_at: u64,
}

/// R-032: how soon a starved listener or datagram endpoint is retried
/// after a refused POST. Nothing is outstanding to complete and wake us --
/// that is what starved means -- so the retry needs a tick of its own. 50ms
/// is short enough that recovery is prompt and long enough that a starved
/// resource that cannot recover does not spin the CPU.
pub(crate) const STARVED_RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// How long a listener backs off after an accept that FAILED (asyncio's
/// `constants.ACCEPT_RETRY_DELAY`). Re-posting at once turned descriptor
/// exhaustion into a hot loop: every re-post failed inline again, so each
/// tick logged another "Accept failed" and spent a core doing it, for as
/// long as the process sat at its descriptor limit.
pub(crate) const ACCEPT_RETRY_DELAY: Duration = Duration::from_secs(1);

/// An accept failure that belongs to the connection being accepted (the
/// client reset or went away first), not to the listener: retried at once,
/// never reported. The epoll backend already retries these inside its own
/// accept; this catches what IOCP reports from a failed AcceptEx.
fn accept_error_is_transient(code: u32) -> bool {
    cadeloop_core::backend::is_transient_accept_error(code)
}

/// R-058 datagram endpoint state (cached protocol callbacks, one
/// outstanding recv, serialized sends).
pub(crate) struct DatagramEntry {
    pub socket: RawSocket,
    /// The socket object the endpoint was opened on. It owns the
    /// descriptor, as asyncio's datagram transport's socket does: the
    /// endpoint closes it (through Python) after connection_lost.
    pub owner: Option<Py<PyAny>>,
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

/// Charged against `DGRAM_SEND_QUEUE_MAX` on top of the payload, so the
/// queue is bounded by depth as well as bytes. Covers what a queued
/// datagram actually costs: a Vec header, an optional SocketAddr, and the
/// VecDeque slot -- none of which is zero for an empty packet.
pub(crate) const DGRAM_PER_PACKET: usize = 128;

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
    pub readers: HashMap<RawSocket, Py<Handle>>,
    pub writers: HashMap<RawSocket, Py<Handle>>,
    pub flush_list: Vec<u64>,
    /// Events awaiting phase-2 dispatch (outside the state cell).
    pub events: Vec<NetEvent>,
    /// Watch-callback handles to enqueue on the reactor ready queue.
    pub ready_scratch: Vec<Py<Handle>>,
    /// The reactor's cached time for the tick being translated (ns), so
    /// completion handlers can schedule retries without a clock read.
    pub now_ns: u64,
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
    /// Watch handles the loop gave up (removed or displaced readers and
    /// writers): released, then dropped, outside the cell.
    pub graveyard_handles: Vec<Py<Handle>>,
    /// Sockets handed out by `get_extra_info("socket")`, awaiting an
    /// explicit `close()` outside the state cell (see drain_graveyards).
    pub graveyard_sockets: Vec<Py<PyAny>>,
    pub graveyard_protos: Vec<ProtoKind>,
    /// R-058 datagram endpoints (separate from stream transports: no
    /// write-queue/backpressure machinery, per-packet semantics).
    pub datagrams: HashMap<u64, DatagramEntry>,
    /// Which transport or datagram endpoint drives each descriptor, so
    /// add_reader()/add_writer() can refuse a transport's fd as asyncio
    /// does: a watch on it would fight the transport's own registration.
    pub fd_users: HashMap<RawSocket, FdUser>,
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
    /// The transport whose `close()` is sending its corked output inline:
    /// if that drains the queue, its connection_lost is still scheduled
    /// with call_soon, as asyncio's close() schedules it (see
    /// `Transport::close`).
    inline_close: Option<u64>,
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

    /// Anything in the graveyards the tick drops in bulk? Lets it skip four
    /// `mem::take`s (and the drop of four empty vectors) on the
    /// overwhelmingly common tick that retired nothing.
    #[inline]
    pub(crate) fn has_graveyards(&self) -> bool {
        !(self.graveyard_entries.is_empty()
            && self.graveyard_bufs.is_empty()
            && self.graveyard_py.is_empty()
            && self.graveyard_protos.is_empty())
    }

    /// Graveyard entries that need more than a drop (sockets to close,
    /// handles to release): only `drain_graveyards` handles these.
    #[inline]
    pub(crate) fn has_special_graveyards(&self) -> bool {
        !(self.graveyard_sockets.is_empty() && self.graveyard_handles.is_empty())
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

/// What a descriptor in `NetState::fd_users` belongs to.
#[derive(Clone, Copy)]
pub(crate) enum FdUser {
    Stream(u64),
    Datagram(u64),
}

impl NetState {
    /// What uses `fd`, if a live transport does: `(false, facade)` for a
    /// stream transport, `(true, connection_lost)` for a datagram
    /// endpoint, whose transport is that bound method's `__self__` (read
    /// by the caller, outside the cell).
    pub(crate) fn transport_for_fd(&self, py: Python<'_>, fd: RawSocket) -> Option<(bool, Py<PyAny>)> {
        match *self.fd_users.get(&fd)? {
            FdUser::Stream(tid) => {
                let e = self.transports.get(&tid)?;
                e.pyobj.as_ref().map(|t| (false, t.clone_ref(py).into_any()))
            }
            FdUser::Datagram(did) => {
                let e = self.datagrams.get(&did)?;
                Some((true, e.connection_lost.clone_ref(py)))
            }
        }
    }
}

/// Phase-2 events: dispatched with the GIL, outside the state cell.
pub(crate) enum NetEvent {
    /// Plain-protocol data: payload prebuilt, slot already re-posted.
    ///
    /// This and the other inbound events below name their transport, not
    /// the callback to call: where the bytes go is decided when the event
    /// is dispatched (`route_inbound`), since a callback that runs between
    /// the poll and then -- in this tick's ready batch, or an earlier
    /// event's -- may close the transport, pause its reading or swap its
    /// protocol, and asyncio, which removes or re-reads the reader at
    /// those moments, delivers nothing to a closed or paused transport and
    /// everything to the current protocol.
    Data {
        tid: u64,
        payload: Py<PyAny>,
    },
    /// R-058: one received datagram (payload prebuilt, recv re-posted).
    DgramData {
        did: u64,
        payload: Py<PyAny>,
        addr: Option<netsys::Addr>,
    },
    /// R-058: per-packet error surfaced to protocol.error_received.
    DgramError {
        did: u64,
        err: u32,
    },
    /// R-058: datagram endpoint torn down. `close_after`: its socket
    /// object, closed once connection_lost has run; `immediate` as for
    /// `ConnLost` (asyncio's `_sendto_ready` finishing a close).
    DgramLost {
        connection_lost: Py<PyAny>,
        err: Option<u32>,
        close_after: CloseOnDrop,
        immediate: bool,
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
    /// The bytes a paused transport held (`paused_recv`), delivered by
    /// `resume_reading`: an owned copy, with no pool slot of its own.
    HeldData {
        tid: u64,
        data: Vec<u8>,
    },
    /// BufferedProtocol data: copy out of the retained slot in phase 2
    /// (get_buffer may run arbitrary Python, so it cannot run in-cell).
    BufData {
        tid: u64,
        slot: SlotId,
        len: usize,
    },
    Eof {
        tid: u64,
    },
    ConnLost(Box<ConnLost>),
    /// `resume_writing`, then -- if that left the queue empty -- the
    /// close or write_eof that was waiting for the drain.
    ResumeWriting {
        tid: u64,
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

/// A socket object a dead transport still holds (the caller's own, or the
/// duplicate behind `get_extra_info("socket")`), closed -- not merely
/// dropped -- whenever it is let go: after connection_lost normally, but
/// also when that never runs because the loop closed first, so a
/// transport's close stays final either way. Dropped attached, outside
/// the state cell, like every Python reference an event carries.
pub(crate) struct CloseOnDrop(Option<Py<PyAny>>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if let Some(sock) = self.0.take() {
            Python::attach(|py| {
                let _ = sock.call_method0(py, intern!(py, "close"));
            });
        }
    }
}

/// A stream transport's teardown, as asyncio's `_call_connection_lost`
/// performs it. Boxed: it is rare, and the hot Data event should not pay
/// for its size.
pub(crate) struct ConnLost {
    connection_lost: Py<PyAny>,
    err: LostReason,
    /// asyncio calls connection_lost straight from the I/O callback that
    /// finished draining a closing transport (`_write_ready`), and
    /// through `call_soon` everywhere else (`_force_close`, an idle
    /// `close()`): true for the former.
    immediate: bool,
    /// The facade transport and its protocol: `get_protocol()` keeps
    /// answering while connection_lost runs, as asyncio's does.
    transport: Option<Py<Transport>>,
    protocol: Py<PyAny>,
    /// Closed after connection_lost: the caller's socket object, or the
    /// duplicate behind `get_extra_info("socket")`.
    close_after: CloseOnDrop,
    server: Option<Py<PyAny>>,
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
        maybe_finish_shutdown(py, net, backend, tid, true);
        return;
    }
    let mut slices: [IoSlice; 16] = [IoSlice { ptr: std::ptr::null(), len: 0 }; 16];
    let mut n = 0;
    // Bounded by bytes as well as slices: the completion's count is 32-bit
    // (see MAX_SEND_BYTES). What does not fit goes in the next post.
    let mut total = 0usize;
    for buf in entry.wq.iter().take(16) {
        let slice = buf.slice();
        if n > 0 && total + slice.len as usize > MAX_SEND_BYTES {
            break;
        }
        total += slice.len as usize;
        slices[n] = slice;
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

/// The drain-time close/eof that `on_send_done` held back for
/// resume_writing: done now unless resume_writing queued more output (a
/// flush is then on its way, and its completion finishes the job). In-cell.
fn finish_after_resume(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, tid: u64) {
    let Some(entry) = net.transports.get(&tid) else { return };
    if entry.wq.is_empty() && entry.send_op.is_none() {
        maybe_finish_shutdown(py, net, backend, tid, true);
    }
}

/// asyncio's `_maybe_pause_protocol` for a queue that just grew, or a
/// high-water mark that just dropped: returns the protocol's
/// pause_writing to call (outside the cell) when the queue is over the
/// mark. In-cell.
///
/// asyncio writes to the socket at once and pauses only for what the
/// kernel would not take. Here a write is queued first, so before
/// deciding, the queue is pushed out and any send the backend finished
/// inline (epoll's writev at post time) is applied. Without that, one
/// large write to an idle socket paused the protocol -- and a tick later
/// resumed it -- although the kernel had taken every byte. The common
/// write, under the mark, costs one lookup.
fn maybe_pause(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, tid: u64) -> Option<Py<PyAny>> {
    let e = net.transports.get(&tid)?;
    if e.proto_paused || e.queued_bytes <= e.high_water {
        return None;
    }
    if e.send_op.is_none() {
        flush_pending(py, net, backend, tid);
    }
    settle_inline(py, net, backend);
    let e = net.transports.get_mut(&tid)?;
    if e.proto_paused || e.queued_bytes <= e.high_water {
        return None;
    }
    e.proto_paused = true;
    match &e.proto {
        ProtoKind::Py(p) => Some(p.pause_writing.clone_ref(py)),
        ProtoKind::Http(_) => None,
    }
}

/// Apply the completions the backend already holds in user space (inline
/// successes; see `IoBackend::take_staged`) now rather than at the next
/// poll. A completed send can post the next, which can complete inline
/// in turn, hence the (bounded) loop. In-cell, like the tick's own
/// translate.
fn settle_inline(py: Python<'_>, net: &mut NetState, backend: Backend<'_>) {
    let mut staged = Vec::new();
    for _ in 0..8 {
        if backend.take_staged(&mut staged) == 0 {
            return;
        }
        translate(py, net, backend, &staged);
        staged.clear();
    }
}

/// eof/close finalization once the write queue is drained. In-cell.
/// `drained`: the queue just emptied on the I/O path (a send completion,
/// the tick's flush), where asyncio's `_write_ready` calls
/// connection_lost directly; a `close()` call itself defers it -- also
/// when it is close()'s own inline send that drained the queue.
fn maybe_finish_shutdown(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, tid: u64, drained: bool) {
    let Some(entry) = net.transports.get_mut(&tid) else { return };
    if entry.send_op.is_some() || !entry.wq.is_empty() || entry.conn_lost {
        return;
    }
    if entry.eof_wanted && !entry.eof_sent {
        entry.eof_sent = true;
        let _ = netsys::shutdown_send(entry.socket);
    }
    if entry.closing {
        let immediate = drained && net.inline_close != Some(tid);
        teardown_lost(py, net, backend, tid, LostReason::Clean, immediate);
    }
}

/// Give a transport's corked output (R-035: queued, but no send posted
/// yet) the send asyncio's write() would already have made. asyncio
/// writes straight to the socket when nothing is queued ahead, so by the
/// time a protocol calls close() or abort() its earlier writes have
/// normally left; only what the kernel refused is still buffered, and
/// only that is held back (close) or discarded (abort). Called before
/// either decides. Returns whether a send was posted. In-cell.
fn send_corked(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, tid: u64) -> bool {
    let Some(e) = net.transports.get(&tid) else { return false };
    if e.conn_lost || e.send_op.is_some() || e.wq.is_empty() {
        return false;
    }
    flush_pending(py, net, backend, tid);
    true
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

/// What a stream transport's `connection_lost` receives.
pub(crate) enum LostReason {
    /// Orderly close: `None`.
    Clean,
    /// An OS error code, raised as the matching OSError subclass.
    Os(u32),
    /// A Python exception, delivered as is. This is how sslproto reports
    /// a failed handshake, a corrupt record or an expired timeout (through
    /// `_force_close(exc)`), and how a raising protocol callback is
    /// reported -- the application learns why the connection died.
    Exc(Py<PyAny>),
}

impl From<Option<u32>> for LostReason {
    fn from(err: Option<u32>) -> Self {
        err.map_or(LostReason::Clean, LostReason::Os)
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
    teardown_reason(py, net, backend, tid, err.into());
}

/// `teardown_with` for any [`LostReason`]. In-cell: an exception that
/// will not be delivered (the transport is already gone) goes to the
/// graveyard rather than being dropped here.
pub(crate) fn teardown_reason(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    tid: u64,
    err: LostReason,
) {
    teardown_lost(py, net, backend, tid, err, false);
}

/// `teardown_reason`, saying whether connection_lost runs at once (see
/// `ConnLost::immediate`).
fn teardown_lost(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    tid: u64,
    err: LostReason,
    immediate: bool,
) {
    let Some(mut entry) = net.transports.remove(&tid) else {
        if let LostReason::Exc(exc) = err {
            net.graveyard_py.push(exc);
        }
        return;
    };
    if entry.conn_lost {
        if let LostReason::Exc(exc) = err {
            net.graveyard_py.push(exc);
        }
        retire_sockets(net, &mut entry);
        net.graveyard_entries.push(entry);
        return;
    }
    entry.conn_lost = true;
    if let Some(t) = &entry.pyobj {
        t.get().mark_lost();
    }
    cancel_transport_ops(net, backend, &mut entry);
    release_socket(backend, &entry);
    forget_fd(net, entry.socket, tid);
    if let Some(fd) = entry.extra_fd.take() {
        forget_fd(net, fd, tid);
    }
    if let Some(slot) = entry.recv_slot.take() {
        // The transport's own reference. The posted op holds a second one
        // (R-073) that only the reap releases.
        net.buffers.release(slot);
    }
    match &mut entry.proto {
        ProtoKind::Py(p) => {
            let lost = ConnLost {
                connection_lost: p.connection_lost.clone_ref(py),
                err,
                immediate,
                transport: entry.pyobj.take(),
                protocol: p.protocol.clone_ref(py),
                close_after: CloseOnDrop(entry.owner.take().or_else(|| entry.extra_sock.take())),
                server: entry.server.take(),
            };
            net.events.push(NetEvent::ConnLost(Box::new(lost)));
        }
        ProtoKind::Http(conn) => {
            if let LostReason::Exc(exc) = err {
                net.graveyard_py.push(exc);
            }
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
            // The queue will never drain now; releasing every parked
            // producer lets each one's next send() raise properly instead
            // of hanging.
            for fut in conn.take_drain_waiters() {
                net.events.push(NetEvent::HttpDrained { fut });
            }
            for fut in conn.take_recv_waiters() {
                net.events.push(NetEvent::HttpDisconnect { fut, ws });
            }
        }
    }
    retire_sockets(net, &mut entry);
    net.graveyard_entries.push(entry);
}

/// Drop `fd` from `fd_users` if `id` (a tid or did) still owns it: a
/// descriptor number the OS has already handed to a newer transport
/// must keep its entry.
fn forget_fd(net: &mut NetState, fd: RawSocket, id: u64) {
    if let Some(FdUser::Stream(owner) | FdUser::Datagram(owner)) = net.fd_users.get(&fd) {
        if *owner == id {
            net.fd_users.remove(&fd);
        }
    }
}

/// Deregister a dead transport's descriptor and close it -- unless the
/// caller's socket object owns it: that is closed through Python instead
/// (`retire_sockets`, or after connection_lost), never under it.
fn release_socket(backend: Backend<'_>, entry: &TransportEntry) {
    backend.detach_socket(entry.socket);
    if entry.owner.is_none() {
        netsys::close(entry.socket);
    }
}

/// Socket objects a dead transport still holds, closed outside the cell
/// (`graveyard_sockets`): closing, not dropping, is what makes the
/// transport's close final for a socket the application may still hold.
fn retire_sockets(net: &mut NetState, entry: &mut TransportEntry) {
    if let Some(sock) = entry.owner.take() {
        net.graveyard_sockets.push(sock);
    }
    if let Some(sock) = entry.extra_sock.take() {
        net.graveyard_sockets.push(sock);
    }
    if let Some(server) = entry.server.take() {
        net.graveyard_py.push(server);
    }
}

/// Used by CoreLoop::close (no Python available for events — they are
/// cleared right after).
pub(crate) fn teardown(net: &mut NetState, backend: Backend<'_>, tid: u64, _err: Option<u32>) {
    let Some(mut entry) = net.transports.remove(&tid) else { return };
    entry.conn_lost = true;
    if let Some(t) = &entry.pyobj {
        t.get().mark_lost();
    }
    cancel_transport_ops(net, backend, &mut entry);
    release_socket(backend, &entry);
    forget_fd(net, entry.socket, tid);
    if let Some(fd) = entry.extra_fd.take() {
        forget_fd(net, fd, tid);
    }
    if let Some(slot) = entry.recv_slot.take() {
        net.buffers.release(slot);
    }
    retire_sockets(net, &mut entry);
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
        // Accepts too: a cancelled AcceptEx owns the socket it pre-created,
        // and only reaping its completion closes it. Waiting on guards and
        // pipes alone left every pooled accept socket of every listener the
        // close tore down -- 64 apiece by default -- open for good.
        let ops_left = net.ops.values().any(|t| {
            matches!(t, OpTarget::PipeRead { .. } | OpTarget::PipeWrite { .. } | OpTarget::Accept(_))
        });
        if net.reap_guards.is_empty() && !ops_left {
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
/// endpoint. Without an `owner` object the engine owns the descriptor
/// from here; with one, that object does, and a failure here leaves it
/// open for the caller.
pub(crate) fn udp_wire(
    net: &mut NetState,
    backend: Backend<'_>,
    sock: RawSocket,
    datagram_received: Py<PyAny>,
    error_received: Py<PyAny>,
    connection_lost: Py<PyAny>,
    owner: Option<Py<PyAny>>,
) -> io::Result<u64> {
    if let Err(e) = backend.register_socket(sock) {
        net.graveyard_py.push(datagram_received);
        net.graveyard_py.push(error_received);
        net.graveyard_py.push(connection_lost);
        if let Some(owner) = owner {
            net.graveyard_py.push(owner);
        }
        return Err(e);
    }
    let did = net.next_id();
    net.fd_users.insert(sock, FdUser::Datagram(did));
    net.datagrams.insert(
        did,
        DatagramEntry {
            socket: sock,
            owner,
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
            forget_fd(net, entry.socket, did);
            match entry.owner.take() {
                Some(owner) => net.graveyard_py.push(owner), // still the caller's
                None => netsys::close(entry.socket),
            }
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
        let ptr = net.buffers.slot_ptr(slot);
        let payload = unsafe {
            let obj = ffi::PyBytes_FromStringAndSize(ptr.cast(), bytes as ffi::Py_ssize_t);
            Bound::from_owned_ptr(py, obj).unbind()
        };
        net.events.push(NetEvent::DgramData { did, payload, addr });
    } else if !is_cancelled_error(os_error) {
        // Per-packet errors (e.g. ECONNREFUSED surfaced by a connected
        // socket after an ICMP unreachable): report and KEEP receiving —
        // asyncio semantics.
        net.events.push(NetEvent::DgramError { did, err: os_error });
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
        net.events.push(NetEvent::DgramError { did, err: os_error });
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
        // Mirrors the charge on the way in; the two must move together or
        // the cap drifts.
        entry.queued_bytes = entry.queued_bytes.saturating_sub(data.len() + DGRAM_PER_PACKET);
        dgram_send_now(net, backend, did, &data, addr.as_ref());
    }
    let Some(entry) = net.datagrams.get_mut(&did) else { return };
    if entry.closing && !entry.conn_lost && entry.send_op.is_none() {
        udp_teardown(py, net, backend, did, None, true);
    }
}

fn dgram_send_now(
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
            net.events.push(NetEvent::DgramError { did, err: e.raw_os_error().unwrap_or(0) as u32 });
        }
    }
}

/// asyncio DatagramTransport.sendto: fire-and-forget; sends serialize
/// through one in-flight op with a queue behind it.
pub(crate) fn udp_sendto(
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
        // Charged per datagram as well as per byte: zero-length sends are
        // valid, cost nothing under a byte-only cap, and every one still
        // appends a Vec and an address to the queue -- so a synchronous
        // producer could grow the supposedly bounded queue without limit
        // before the loop ever saw the first completion.
        if entry.queued_bytes + data.len() + DGRAM_PER_PACKET > DGRAM_SEND_QUEUE_MAX {
            // Deterministic overflow policy: drop and report. Growing
            // without bound is the alternative, and a producer that can
            // outrun the socket would take the process down with it.
            net.events.push(NetEvent::DgramError { did, err: ENOBUFS });
            return Ok(());
        }
        entry.queued_bytes += data.len() + DGRAM_PER_PACKET;
        entry.send_queue.push_back((data.to_vec(), addr));
        return Ok(());
    }
    dgram_send_now(net, backend, did, data, addr.as_ref());
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
        udp_teardown(py, net, backend, did, None, false);
    }
    // else: on_dgram_send_done finishes the close once the queue drains.
}

/// Loop-close variant: no Python events (they are cleared right after);
/// refs drop via the graveyard outside the cell.
pub(crate) fn udp_teardown_at_close(net: &mut NetState, backend: Backend<'_>, did: u64) {
    let Some(mut entry) = net.datagrams.remove(&did) else { return };
    cancel_dgram_ops(net, backend, &mut entry);
    backend.detach_socket(entry.socket);
    forget_fd(net, entry.socket, did);
    match entry.owner.take() {
        Some(owner) => net.graveyard_sockets.push(owner),
        None => netsys::close(entry.socket),
    }
    if let Some(slot) = entry.recv_slot.take() {
        net.buffers.release(slot);
    }
    net.graveyard_py.push(entry.datagram_received);
    net.graveyard_py.push(entry.error_received);
    net.graveyard_py.push(entry.connection_lost);
}

fn udp_teardown(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    did: u64,
    err: Option<u32>,
    immediate: bool,
) {
    let Some(mut entry) = net.datagrams.remove(&did) else { return };
    if entry.conn_lost {
        return;
    }
    entry.conn_lost = true;
    cancel_dgram_ops(net, backend, &mut entry);
    backend.detach_socket(entry.socket);
    forget_fd(net, entry.socket, did);
    if entry.owner.is_none() {
        netsys::close(entry.socket);
    }
    if let Some(slot) = entry.recv_slot.take() {
        net.buffers.release(slot);
    }
    net.events.push(NetEvent::DgramLost {
        connection_lost: entry.connection_lost.clone_ref(py),
        err,
        close_after: CloseOnDrop(entry.owner.take()),
        immediate,
    });
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

/// Fallback reclaim deadline for a connection whose peer has gone while
/// idle timeouts are disabled (`keepalive_idle=0`). A dead peer must never
/// be pinned forever just because keep-alive was told to be unlimited, so
/// the disconnect reclaim (H4) falls back to this when there is no idle
/// deadline to borrow. Generous, because we cannot tell a full close from
/// a half-close whose app is still producing a response.
const DISCONNECTED_RECLAIM_FALLBACK_NS: u64 = 60 * 1_000_000_000;

/// Sweep phases. `Dead` (peer disconnected, nothing left to send) is
/// checked before `Busy`: an app parked on a non-receive() awaitable while
/// its client is gone is "busy" by every other measure, and without this
/// would pin the fd and its whole request state with no deadline at all.
const PHASE_IDLE: u8 = 0;
const PHASE_HEAD: u8 = 1;
const PHASE_BUSY: u8 = 2;
const PHASE_DEAD: u8 = 3;

pub(crate) fn http_sweep(
    py: Python<'_>,
    net: &mut NetState,
    backend: Backend<'_>,
    now_ns: u64,
) -> (u32, u32, Vec<Py<PyAny>>) {
    let mut expire_head: Vec<u64> = Vec::new();
    let mut expire_idle: Vec<u64> = Vec::new();
    let mut expire_dead: Vec<u64> = Vec::new();
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
        // Nothing is queued or in flight for the peer (checked on `entry`,
        // before the borrow of `entry.proto` below).
        let nothing_to_send = entry.wq.is_empty() && entry.send_op.is_none();
        let ProtoKind::Http(conn) = &mut entry.proto else { continue };
        // `!saw_bytes` is the head phase too: a connection that has sent
        // nothing at all reports `in_head() == false`, and calling that
        // keep-alive idle exempted it from the head deadline it has never
        // yet satisfied. It must be BYTES rather than a finished request,
        // or a body upload counts as a head that never completes.
        let phase: u8 = if conn.disconnected && nothing_to_send {
            // Peer EOF'd and we have nothing left to deliver. A well-behaved
            // app observes http.disconnect through receive() and returns at
            // once (its waiters were resolved at EOF); one parked on an
            // unrelated awaitable would otherwise hold the fd, the AppTask
            // and the coroutine frame indefinitely, since `busy` exempts an
            // active request from every deadline. Reclaim it.
            PHASE_DEAD
        } else if busy {
            PHASE_BUSY
        } else if conn.parser.in_head() || !conn.saw_bytes {
            PHASE_HEAD
        } else {
            PHASE_IDLE
        };
        let moved = conn.activity != conn.sweep_seen;
        conn.sweep_seen = conn.activity;
        if phase != conn.sweep_phase
            || conn.sweep_anchor_ns == 0
            || phase == PHASE_BUSY
            || (phase == PHASE_IDLE && moved)
        {
            // New phase, first sighting, busy, or fresh idle activity:
            // (re)anchor. A head in progress deliberately does NOT
            // re-anchor on activity — that is the slowloris rule. PHASE_DEAD
            // also does not re-anchor (the peer is gone; nothing it does
            // can extend its grace).
            conn.sweep_phase = phase;
            conn.sweep_anchor_ns = now_ns;
            continue;
        }
        let elapsed = now_ns.saturating_sub(conn.sweep_anchor_ns);
        match phase {
            PHASE_HEAD if conn.head_timeout_ns > 0 && elapsed >= conn.head_timeout_ns => {
                expire_head.push(tid)
            }
            PHASE_IDLE if conn.idle_timeout_ns > 0 && elapsed >= conn.idle_timeout_ns => {
                expire_idle.push(tid)
            }
            PHASE_DEAD => {
                let deadline = if conn.idle_timeout_ns > 0 {
                    conn.idle_timeout_ns
                } else {
                    DISCONNECTED_RECLAIM_FALLBACK_NS
                };
                if elapsed >= deadline {
                    expire_dead.push(tid);
                }
            }
            _ => {}
        }
    }
    // A disconnected connection whose eager app is still parked: tear it
    // down (frees the fd and the transport entry) and hand its driver back
    // to the caller to cancel OUTSIDE the state cell. Cancelling unwinds
    // the coroutine (CancelledError into its awaited future), which both
    // reclaims the AppTask + frame -- a plain `Py<AppTask>` is opaque to
    // the cycle collector, so dropping `conn.driver` alone would not, the
    // awaited future's `_wake` still references it -- and gives the app its
    // `finally`/cancel-scope cleanup.
    let mut cancel_drivers: Vec<Py<PyAny>> = Vec::new();
    for &tid in &expire_dead {
        if let Some(conn) = net.http_conn_mut(tid) {
            if let Some(driver) = conn.driver.take() {
                cancel_drivers.push(driver.into_any());
            }
        }
        teardown_with(py, net, backend, tid, None);
    }
    for &tid in &expire_head {
        // A TLS session that never completed its handshake cannot be
        // answered: the 408 would stage as plaintext no cipher state can
        // ever encrypt, close_after_write defers behind that stage, and
        // every later sweep appended another 408 -- socket and receive
        // slot held forever, free amplification for a peer that connects
        // and sends nothing. Tear it down instead.
        let handshaking =
            net.http_conn_mut(tid).and_then(|c| c.tls.as_ref()).map(|t| t.handshaking).unwrap_or(false);
        if handshaking {
            teardown_with(py, net, backend, tid, None);
            continue;
        }
        // 408 then close: the head never completed inside the window.
        let minor = net.http_conn_mut(tid).and_then(|c| c.parser.http_version()).map(|(_, m)| m).unwrap_or(1);
        let resp = crate::http::error_response(ParseError { status: 408, reason: "Request Timeout" }, minor);
        http_enqueue(py, net, backend, tid, resp);
        http_close_after_write(py, net, backend, tid);
    }
    for &tid in &expire_idle {
        // Idle keep-alive expiry closes silently (uvicorn-compatible).
        teardown_with(py, net, backend, tid, None);
    }
    // Disconnect reclaims count as idle closes for the facade's stats.
    let idle = (expire_idle.len() + expire_dead.len()) as u32;
    (expire_head.len() as u32, idle, cancel_drivers)
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
        // Send the 1012 close frame to the peer, then deliver
        // websocket.disconnect(1012) to the application. Waking the app
        // (rather than queuing the disconnect and tearing down here) does
        // two things: the app actually RECEIVES the disconnect it is
        // entitled to, and teardown moves to the normal path -- the app
        // returns, `ws_app_done` closes after the frame has drained. The
        // old code tore the connection down in the same tick it produced
        // the frame; with the base's inline completions that could drop
        // the socket before the just-written close frame was delivered,
        // so the peer saw a bare TCP close and the app never saw its
        // disconnect. If the app is NOT parked in receive(), there is no
        // waiter to wake, so fall back to closing once the frame drains.
        http_enqueue(py, net, backend, tid, cadeloop_core::ws::close_frame(1012, "server shutdown"));
        let waiters = net.http_conn_mut(tid).map(|c| c.take_recv_waiters()).unwrap_or_default();
        if let Some(conn) = net.http_conn_mut(tid) {
            if let Some(wsc) = conn.ws.as_mut() {
                wsc.closing = true;
                // What outlives the connection (a receive() after its
                // teardown, a waiter whose wake was still queued) reports
                // the code this end closed with, not 1006.
                wsc.close_code = Some(1012);
                let m = crate::http::WsMsg::Disconnect(1012);
                wsc.inbox_bytes += m.charge();
                wsc.inbox.push_back(m);
            }
        }
        if waiters.is_empty() {
            http_close_after_write(py, net, backend, tid);
        } else {
            for fut in waiters {
                net.events.push(NetEvent::WsWake { tid, fut });
            }
        }
    }
    for tid in idle {
        teardown_with(py, net, backend, tid, None);
    }
    busy
}

/// Graceful shutdown, last phase: the eager request drivers (AppTasks)
/// still in flight, for the caller to cancel with the tasks the
/// application left running. They are not asyncio Tasks, so the
/// `asyncio.all_tasks()` sweep that does that never saw them: a handler
/// still awaiting at the deadline was never cancelled, and its cleanup
/// never ran. In-cell; clones only.
pub(crate) fn http_request_drivers(py: Python<'_>, net: &NetState) -> Vec<Py<PyAny>> {
    net.transports
        .values()
        .filter_map(|e| match &e.proto {
            ProtoKind::Http(conn) => conn.driver.as_ref().map(|d| d.clone_ref(py).into_any()),
            ProtoKind::Py(_) => None,
        })
        .collect()
}

/// Everything still parked on a native HTTP connection -- its eager
/// driver, its receive()/send() waiters, and waiters already taken into
/// events that will now never be dispatched -- for `CoreLoop::close` to
/// cancel before the teardown drops it. Dropped pending, a driver and the
/// future it awaited kept each other alive through a cycle the collector
/// cannot see (AppTask is opaque to it), so the handler's coroutine was
/// never finalized. In-cell; clones only.
pub(crate) fn http_parked(py: Python<'_>, net: &NetState) -> Vec<Py<PyAny>> {
    let mut parked = http_request_drivers(py, net);
    for e in net.transports.values() {
        if let ProtoKind::Http(conn) = &e.proto {
            parked.extend(conn.recv_waiters.iter().chain(&conn.drain_waiters).map(|f| f.clone_ref(py)));
        }
    }
    for ev in &net.events {
        if let NetEvent::HttpDisconnect { fut, .. }
        | NetEvent::HttpDrained { fut }
        | NetEvent::WsWake { fut, .. } = ev
        {
            parked.push(fut.clone_ref(py));
        }
    }
    parked
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
    maybe_finish_shutdown(py, net, backend, tid, true);
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

pub(crate) fn listener_create(
    net: &mut NetState,
    sock: RawSocket,
    kind: ListenerKind,
    target: usize,
    owner: Option<Py<PyAny>>,
) -> u64 {
    let lid = net.next_id();
    net.listeners.insert(
        lid,
        ListenerEntry {
            socket: sock,
            kind,
            owner,
            server: None,
            accept_ops: Vec::new(),
            target: target.max(1),
            closing: false,
            starved: false,
            retry_at: 0,
        },
    );
    lid
}

/// Attach the asyncio Server facade to a listener (see
/// `ListenerEntry::server`). Returns the previous one, for the caller to
/// drop outside the cell.
pub(crate) fn listener_set_server(net: &mut NetState, lid: u64, server: Py<PyAny>) -> Option<Py<PyAny>> {
    match net.listeners.get_mut(&lid) {
        Some(l) => l.server.replace(server),
        None => Some(server),
    }
}

/// Arm a listener's accept pool. Fails loudly when not a single accept
/// could be posted: a listener with nothing outstanding accepts nothing,
/// ever, and returning "serving" for it hides the outage completely.
pub(crate) fn listener_start(net: &mut NetState, backend: Backend<'_>, lid: u64) -> io::Result<()> {
    // Not reported: a listener that cannot post its first accept fails
    // the call that started it, which is the report.
    post_accepts_inner(net, backend, lid, false);
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
pub(crate) fn retry_starved_listeners(net: &mut NetState, backend: Backend<'_>, now_ns: u64) {
    if !net.any_starved_listener {
        return;
    }
    let mut waiting = false;
    let starved: Vec<u64> = net
        .listeners
        .iter()
        .filter(|(_, l)| l.starved && !l.closing)
        .filter(|(_, l)| {
            let due = l.retry_at <= now_ns;
            waiting |= !due;
            due
        })
        .map(|(&lid, _)| lid)
        .collect();
    let starved_dgrams: Vec<u64> = net
        .datagrams
        .iter()
        .filter(|(_, e)| e.recv_starved && !e.closing && !e.conn_lost)
        .map(|(&did, _)| did)
        .collect();
    // Listeners still inside their backoff keep the flag up.
    net.any_starved_listener = waiting;
    for lid in starved {
        if let Some(l) = net.listeners.get_mut(&lid) {
            l.starved = false;
        }
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

/// How long the tick may park before a starved listener or datagram
/// endpoint is due for its retry (`STARVED_RETRY_INTERVAL` at most).
pub(crate) fn starved_retry_in(net: &NetState, now_ns: u64) -> Duration {
    let soonest = net
        .listeners
        .values()
        .filter(|l| l.starved && !l.closing)
        .map(|l| l.retry_at.saturating_sub(now_ns))
        .min()
        .unwrap_or(0);
    // Datagram endpoints carry no deadline of their own: they retry on the
    // plain interval, as do listeners with nothing scheduled.
    let any_dgram = net.datagrams.values().any(|e| e.recv_starved && !e.closing && !e.conn_lost);
    let listener_wait = Duration::from_nanos(soonest).min(ACCEPT_RETRY_DELAY);
    let wait = if any_dgram { listener_wait.min(STARVED_RETRY_INTERVAL) } else { listener_wait };
    wait.max(Duration::from_millis(1))
}

pub(crate) fn listener_teardown(net: &mut NetState, backend: Backend<'_>, lid: u64) {
    listener_remove(net, backend, lid, true);
}

/// `listener_teardown`, except that `close_owner: false` leaves the
/// caller's socket object open: a create_server(sock=...) that failed
/// hands the socket back untouched, as asyncio does.
pub(crate) fn listener_remove(net: &mut NetState, backend: Backend<'_>, lid: u64, close_owner: bool) {
    let Some(mut listener) = net.listeners.remove(&lid) else { return };
    listener.closing = true;
    for op in listener.accept_ops.drain(..) {
        let _ = backend.cancel(op);
        // Mapping kept: `on_accept_done` already knows how to reap a
        // socket AcceptEx produced for a listener that has since closed,
        // but only if the completion still resolves to this lid.
    }
    backend.detach_socket(listener.socket);
    match listener.owner {
        Some(owner) if close_owner => net.graveyard_sockets.push(owner),
        Some(owner) => net.graveyard_py.push(owner),
        None => netsys::close(listener.socket),
    }
    if let Some(server) = listener.server {
        net.graveyard_py.push(server);
    }
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
    post_accepts_inner(net, backend, lid, true);
}

fn post_accepts_inner(net: &mut NetState, backend: Backend<'_>, lid: u64, report: bool) {
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
            Err(e) => {
                // With at least one accept still outstanding its completion
                // retries this. With none, nothing ever would -- the
                // listener would go permanently deaf while still reporting
                // itself as serving -- so flag it for the tick to re-arm.
                //
                // WouldBlock is the pool being full (a backend that parks
                // one accept per listener), not a failure. Anything else is
                // the accept itself failing -- descriptor or memory
                // exhaustion, most often -- which is reported once and
                // retried after ACCEPT_RETRY_DELAY rather than on the next
                // tick, where it would fail the same way again.
                let failed = e.kind() != io::ErrorKind::WouldBlock;
                let now = net.now_ns;
                let l = net.listeners.get_mut(&lid).unwrap();
                if l.accept_ops.is_empty() {
                    let delay = if failed { ACCEPT_RETRY_DELAY } else { STARVED_RETRY_INTERVAL };
                    l.starved = true;
                    l.retry_at = now.saturating_add(delay.as_nanos() as u64);
                    net.any_starved_listener = true;
                    net.stats_accept_starved += 1;
                    if failed && report {
                        let err = e.raw_os_error().map_or(5, |c| c as u32);
                        net.events.push(NetEvent::AcceptError { err });
                    }
                }
                return;
            }
        }
    }
}

/// Drop the cancelled watches `translate` found on `socket`: the backend
/// stops watching those sides and the handles go to the graveyard, to be
/// released outside the cell. In-cell.
fn unwatch_cancelled(net: &mut NetState, backend: Backend<'_>, socket: RawSocket, r: bool, w: bool) {
    let old_r = if r { net.readers.remove(&socket) } else { None };
    let old_w = if w { net.writers.remove(&socket) } else { None };
    let (keep_r, keep_w) = (net.readers.contains_key(&socket), net.writers.contains_key(&socket));
    if backend.set_watch(socket, keep_r, keep_w).is_err() {
        // Still watched: keep the tables agreeing with the backend, as
        // remove_reader() does, and try again on the next event.
        if let Some(h) = old_r {
            net.readers.insert(socket, h);
        }
        if let Some(h) = old_w {
            net.writers.insert(socket, h);
        }
        return;
    }
    net.graveyard_handles.extend(old_r);
    net.graveyard_handles.extend(old_w);
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
                // A watch whose Handle was cancelled -- Handle.cancel() on
                // what loop._add_reader()/_add_writer() return -- is dropped
                // the first time its fd fires, as asyncio's _process_events
                // drops it. Left in place it was queued (and skipped) on
                // every event, and an fd that stayed ready kept the loop
                // spinning instead of parking.
                let cancelled = |h: Option<&Py<Handle>>| h.is_some_and(|h| h.get().is_cancelled());
                let drop_r = readable && cancelled(net.readers.get(&socket));
                let drop_w = writable && cancelled(net.writers.get(&socket));
                if drop_r || drop_w {
                    unwatch_cancelled(net, backend, socket, drop_r, drop_w);
                }
                if readable && !drop_r {
                    if let Some(h) = net.readers.get(&socket) {
                        let h = h.clone_ref(py);
                        net.ready_scratch.push(h);
                    }
                }
                if writable && !drop_w {
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
                if let Some(guard) =
                    if net.reap_guards.is_empty() { None } else { net.reap_guards.remove(&op) }
                {
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
        let (http_idle, waiters) = match &mut entry.proto {
            ProtoKind::Py(_) => {
                if entry.reading_paused {
                    // Held, as data completing while paused is: delivered
                    // by resume_reading, after anything held before it.
                    entry.paused_eof = true;
                } else {
                    // Phase 2 decides close-vs-keep from eof_received().
                    net.events.push(NetEvent::Eof { tid });
                }
                return;
            }
            ProtoKind::Http(conn) => {
                // Idle keep-alive connection: tear down now. Mid-request:
                // keep writing the response; finish_request closes it.
                conn.disconnected = true;
                let ws = conn.ws.is_some();
                ((!conn.active && conn.pending.is_empty(), ws), conn.take_recv_waiters())
            }
        };
        let (http_idle, waiter_ws) = (http_idle.0, http_idle.1);
        for fut in waiters {
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
    let mut oversize_ws = false;
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
            if p.data_received.is_some() {
                // Plain Protocol: materialize bytes now (non-GC-tracked
                // allocation, safe in-cell) and re-post into the SAME slot
                // immediately so the kernel refills while Python processes.
                let ptr = net.buffers.slot_ptr(slot);
                let payload = unsafe {
                    let obj = ffi::PyBytes_FromStringAndSize(ptr.cast(), bytes as ffi::Py_ssize_t);
                    Bound::from_owned_ptr(py, obj).unbind()
                };
                net.events.push(NetEvent::Data { tid, payload });
            } else {
                // BufferedProtocol: the transport's slot reference transfers
                // to the phase-2 event (released there after the copy); a
                // fresh slot is acquired for the next read.
                entry.recv_slot = None; // ref now owned by the BufData event
                net.events.push(NetEvent::BufData { tid, slot, len: bytes as usize });
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
            } else if conn.deferred_error.is_none() {
                // In-cell parse (R-080): llhttp over the recv slot; the
                // parser copies what it keeps, so the slot re-posts below.
                // (Once a parse error is parked behind requests still being
                // answered, the parser is dead: what arrives is discarded
                // unread, but the recv still re-posts -- reading is the
                // only way to see the peer leave. Stopped, a long-poll
                // handler parked in receive() for exactly that was never
                // told, and the sweep, seeing a request still active, held
                // the connection for good.)
                match crate::http::conn_feed(conn, data) {
                    Ok(outcome) => {
                        // Oversized pre-dispatch WS buffer (see conn_feed):
                        // handled after the match, since teardown needs the
                        // whole NetState and `conn` still borrows it here.
                        oversize_ws = outcome.oversize_ws_trailing;
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
    if oversize_ws {
        // R-087: a client flooded ws_trailing past WS_MAX_MESSAGE before
        // its upgrade was even dispatched. A 413 cannot be injected safely
        // -- an unrelated request on this connection may be mid-response --
        // so the abusive connection is simply dropped.
        teardown_with(py, net, backend, tid, None);
        return;
    }
    if let Some(err) = parse_err {
        // R-086: malformed request answered entirely in-cell -- in its
        // pipeline position (conn_parse_failed): requests already being
        // served keep the response order, and valid ones drained from
        // this same buffer are pumped ahead of the parked 400.
        if crate::http::conn_parse_failed(py, net, backend, tid, err) {
            net.events.push(NetEvent::HttpPump { tid });
        }
        // The parser is dead. An idle connection was answered at once and
        // is closing, so post_recv declines it; one whose 400 is parked
        // keeps reading, discarding what arrives (above).
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

/// Deliver the bytes and EOF a paused transport held, if any, in that
/// order; returns whether the EOF was among them (nothing is left to read
/// then). In-cell; the actual protocol calls happen in phase 2 like every
/// other receive.
fn flush_paused_recv(net: &mut NetState, tid: u64) -> bool {
    let Some(entry) = net.transports.get_mut(&tid) else { return false };
    if entry.conn_lost || entry.closing || !matches!(entry.proto, ProtoKind::Py(_)) {
        entry.paused_recv = None;
        entry.paused_eof = false;
        return false;
    }
    let eof = std::mem::take(&mut entry.paused_eof);
    if let Some(data) = entry.paused_recv.take() {
        net.events.push(NetEvent::HeldData { tid, data });
    }
    if eof {
        net.events.push(NetEvent::Eof { tid });
    }
    eof
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
    if !flush_paused_recv(net, tid) {
        post_recv(py, net, backend, tid);
    }
}

/// Queued write bytes and the high-water mark for a transport (R-084).
///
/// TLS connections stage PLAINTEXT in `TlsState.staged` and only move it
/// to the wire queue on a later TlsFlush, so `queued_bytes` alone reads
/// as zero for HTTPS/WSS however much the producer has written -- the
/// backpressure would have been inert on exactly the connections whose
/// encryption makes them slowest. Count both.
/// Returns (queued including staged plaintext, high water, low water).
/// The low mark rides along because a caller deciding whether to RESUME
/// needs the same staged-inclusive number the pause decision used; two
/// accessors would have let those drift apart.
pub(crate) fn write_pressure(net: &NetState, tid: u64) -> Option<(usize, usize, usize)> {
    net.transports.get(&tid).map(|e| {
        let staged = match &e.proto {
            ProtoKind::Http(conn) => conn.tls.as_ref().map(|t| t.staged.len()).unwrap_or(0),
            ProtoKind::Py(_) => 0,
        };
        (e.queued_bytes + staged, e.high_water, e.low_water)
    })
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
    if let Some(t) = &entry.pyobj {
        t.get().queued.store(entry.queued_bytes, Ordering::Relaxed);
    }
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
    // R-084: release the ASGI producers parked on the high-water mark --
    // all of them, in the order they parked, as asyncio's FlowControlMixin
    // resumes every drain() waiter -- counting staged plaintext, exactly as
    // write_pressure does, so none is woken while the TLS staging buffer
    // is still full.
    if let ProtoKind::Http(conn) = &mut entry.proto {
        let staged = conn.tls.as_ref().map(|t| t.staged.len()).unwrap_or(0);
        if entry.queued_bytes + staged <= entry.low_water {
            for fut in conn.take_drain_waiters() {
                net.events.push(NetEvent::HttpDrained { fut });
            }
        }
    }
    let mut resuming = false;
    if entry.proto_paused && entry.queued_bytes <= entry.low_water {
        entry.proto_paused = false;
        if let ProtoKind::Py(p) = &entry.proto {
            let resume_writing = p.resume_writing.clone_ref(py);
            net.events.push(NetEvent::ResumeWriting { tid, resume_writing });
            resuming = true;
        }
    }
    // Not `unwrap()`. It was safe only while nothing between the lookup
    // above and here could remove the entry, and the WebSocket read-pause
    // re-check below breaks that: resuming a read posts a recv, and a
    // failing post tears the transport down. An invariant that holds by
    // accident of ordering is one edit from a panic -- and a panic here
    // unwinds through FFI, taking the worker with it.
    let Some(entry) = net.transports.get_mut(&tid) else { return };
    if !entry.wq.is_empty() {
        flush_pending(py, net, backend, tid);
    } else if !resuming {
        maybe_finish_shutdown(py, net, backend, tid, true);
    }
    // else: asyncio calls resume_writing BEFORE deciding the queue has
    // drained, and what it writes still goes out ahead of a pending close
    // -- sslproto's close_notify under backpressure is written exactly
    // there. Its dispatch finishes the close (`finish_after_resume`).
    // A WebSocket paused for WRITE pressure has no other way back: the
    // inbox path only re-evaluates when the app calls receive(), and a
    // connection paused because we owed the peer output may have no
    // inbox message for it to consume. Without this the pause added for
    // the ping-flood bound was permanent. Last, so nothing after it
    // depends on the transport still existing.
    crate::http::ws_sync_read_pause(py, net, backend, tid);
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
        if closing {
            return;
        }
        if is_cancelled_error(os_error) || accept_error_is_transient(os_error) {
            // Re-arm even for an ABORTED completion: this op has left the
            // pool either way, and if it was the last one the listener
            // would otherwise stop accepting for good.
            post_accepts(net, backend, lid);
            return;
        }
        // A real failure (descriptor or memory exhaustion, most often):
        // report it once, and if this was the pool's last op, back off
        // before re-posting -- see ACCEPT_RETRY_DELAY. Other ops still
        // outstanding (IOCP posts a pool) carry on undisturbed.
        net.events.push(NetEvent::AcceptError { err: os_error });
        let now = net.now_ns;
        if let Some(l) = net.listeners.get_mut(&lid) {
            if l.accept_ops.is_empty() {
                l.starved = true;
                l.retry_at = now.saturating_add(ACCEPT_RETRY_DELAY.as_nanos() as u64);
                net.any_starved_listener = true;
                net.stats_accept_starved += 1;
            }
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
///
/// A fatal error must not take the rest of the batch with it: the
/// undispatched tail carries waiter futures, buffer slots, and lifecycle
/// callbacks that would otherwise be lost for good. It goes back to the
/// FRONT of the pending-event queue (ahead of anything pushed while this
/// batch ran), so the next tick dispatches it in the original order --
/// the same unwind contract the ready-callback batch keeps.
/// Where a stream transport's inbound bytes go, decided when their event
/// is dispatched (see `NetEvent::Data`).
enum Inbound {
    /// Closed, aborted or gone: asyncio stopped reading at that moment.
    Discard,
    /// Paused: added to the bytes `resume_reading` delivers.
    Held,
    /// The current protocol's `data_received`.
    Plain(Py<PyAny>),
    /// The current protocol's `get_buffer` and `buffer_updated`.
    Buffered(Py<PyAny>, Py<PyAny>),
}

/// Route `len` bytes at `src` that arrived on `tid` (in-cell; copies them
/// only to hold them).
fn route_inbound(py: Python<'_>, net: &mut NetState, tid: u64, src: *const u8, len: usize) -> Inbound {
    let Some(entry) = net.transports.get_mut(&tid) else { return Inbound::Discard };
    if entry.conn_lost || entry.closing {
        return Inbound::Discard;
    }
    let ProtoKind::Py(p) = &entry.proto else { return Inbound::Discard };
    if entry.reading_paused {
        // SAFETY: the caller's event owns the bytes for this call.
        let data = unsafe { std::slice::from_raw_parts(src, len) };
        match &mut entry.paused_recv {
            Some(held) => held.extend_from_slice(data),
            None => entry.paused_recv = Some(data.to_vec()),
        }
        return Inbound::Held;
    }
    match (&p.data_received, &p.get_buffer, &p.buffer_updated) {
        (Some(data_received), _, _) => Inbound::Plain(data_received.clone_ref(py)),
        (None, Some(get_buffer), Some(buffer_updated)) => {
            Inbound::Buffered(get_buffer.clone_ref(py), buffer_updated.clone_ref(py))
        }
        _ => Inbound::Discard,
    }
}

/// Hand routed bytes to the protocol: `payload` when they are already a
/// bytes object, else a new one for a plain protocol (held bytes, or a
/// buffered protocol's slot after set_protocol swapped in a plain one).
/// A failing callback is fatal to the transport, as in asyncio.
fn deliver_inbound(
    py: Python<'_>,
    core: &CoreLoop,
    tid: u64,
    route: Inbound,
    payload: Option<&Py<PyAny>>,
    src: *const u8,
    len: usize,
) -> PyResult<()> {
    match route {
        Inbound::Discard | Inbound::Held => Ok(()),
        Inbound::Plain(data_received) => {
            let res = match payload {
                Some(payload) => data_received.call1(py, (payload,)),
                None => {
                    // SAFETY: the caller's event owns the bytes for this call.
                    let data = unsafe { std::slice::from_raw_parts(src, len) };
                    data_received.call1(py, (pyo3::types::PyBytes::new(py, data),))
                }
            };
            fatal_protocol_error(py, core, tid, res, DATA_RECEIVED_FAILED)
        }
        Inbound::Buffered(get_buffer, buffer_updated) => {
            match copy_into_app_buffer(py, &get_buffer, &buffer_updated, src, len) {
                Ok(()) => Ok(()),
                Err((message, e)) => fatal_protocol_error::<()>(py, core, tid, Err(e), message),
            }
        }
    }
}

/// The current protocol's `eof_received` and the transport, when the peer's
/// EOF is to be delivered now: not after close() or abort(), and while
/// reading is paused it is held instead (see `paused_eof`).
fn route_eof(py: Python<'_>, net: &mut NetState, tid: u64) -> Option<(Py<PyAny>, Py<Transport>)> {
    let entry = net.transports.get_mut(&tid)?;
    if entry.conn_lost || entry.closing {
        return None;
    }
    let ProtoKind::Py(p) = &entry.proto else { return None };
    if entry.reading_paused {
        entry.paused_eof = true;
        return None;
    }
    Some((p.eof_received.clone_ref(py), entry.pyobj.as_ref()?.clone_ref(py)))
}

/// Dispatch `events` in order, leaving the Vec empty (its buffer is the
/// tick's to reuse). A fatal error puts what is left back at the front of
/// the queue.
pub(crate) fn dispatch_events(
    py: Python<'_>,
    slf: &Bound<'_, CoreLoop>,
    events: &mut Vec<NetEvent>,
) -> PyResult<()> {
    let mut events = events.drain(..);
    while let Some(event) = events.next() {
        if let Err(e) = dispatch_one_event(py, slf, event) {
            let rest: Vec<NetEvent> = events.collect();
            if !rest.is_empty() {
                slf.get().with_net(|net, _| {
                    let mut later = std::mem::take(&mut net.events);
                    net.events = rest;
                    net.events.append(&mut later);
                })?;
            }
            return Err(e);
        }
    }
    Ok(())
}

fn dispatch_one_event(py: Python<'_>, slf: &Bound<'_, CoreLoop>, event: NetEvent) -> PyResult<()> {
    let core = slf.get();
    {
        match event {
            NetEvent::Data { tid, payload } => {
                // SAFETY: the event owns `payload`, a bytes object, until
                // the end of this arm.
                let (src, len) = unsafe {
                    (
                        ffi::PyBytes_AsString(payload.as_ptr()).cast::<u8>(),
                        ffi::PyBytes_Size(payload.as_ptr()) as usize,
                    )
                };
                let route = core.with_net(|net, _| route_inbound(py, net, tid, src, len))?;
                deliver_inbound(py, core, tid, route, Some(&payload), src, len)?;
            }
            NetEvent::HeldData { tid, data } => {
                let route = core.with_net(|net, _| route_inbound(py, net, tid, data.as_ptr(), data.len()))?;
                deliver_inbound(py, core, tid, route, None, data.as_ptr(), data.len())?;
            }
            NetEvent::BufData { tid, slot, len } => {
                // The slot is the event's until released below, so its
                // memory stays put across the protocol calls.
                let (route, src) = core.with_net(|net, _| {
                    let src = net.buffers.slot_ptr(slot).cast_const();
                    (route_inbound(py, net, tid, src, len), src)
                })?;
                let res = deliver_inbound(py, core, tid, route, None, src, len);
                core.with_net(|net, _| {
                    net.buffers.release(slot);
                })?;
                res?;
            }
            NetEvent::Eof { tid } => {
                let route = core.with_net(|net, _| route_eof(py, net, tid))?;
                if let Some((eof_received, transport)) = route {
                    match eof_received.call0(py) {
                        Ok(keep_open) => {
                            if !keep_open.is_truthy(py).unwrap_or(false) {
                                transport.bind(py).get().close(py)?;
                            }
                        }
                        Err(e) => {
                            // Fatal, as it is in the stdlib. The receive side
                            // has reached EOF and will never be posted again,
                            // so a transport left open here is half-open
                            // forever unless the application happens to close
                            // it -- and it has just told us it cannot handle
                            // the event.
                            fatal_protocol_error::<()>(py, core, tid, Err(e), EOF_RECEIVED_FAILED)?;
                        }
                    }
                }
            }
            NetEvent::ConnLost(lost) => {
                if lost.immediate {
                    let res = call_connection_lost(py, *lost);
                    core.guard_protocol_call(py, res)?;
                } else {
                    schedule_lost(py, core, Lost::Stream(lost))?;
                }
            }
            NetEvent::DgramData { did, payload, addr } => {
                // Not after close() or abort(): asyncio removed the reader.
                let datagram_received = core.with_net(|net, _| {
                    net.datagrams
                        .get(&did)
                        .filter(|e| !e.closing && !e.conn_lost)
                        .map(|e| e.datagram_received.clone_ref(py))
                })?;
                if let Some(datagram_received) = datagram_received {
                    let addr_obj = any_addr_tuple(py, addr).unwrap_or_else(|| py.None());
                    core.guard_protocol_call(py, datagram_received.call1(py, (payload, addr_obj)))?;
                }
            }
            NetEvent::DgramError { did, err } => {
                // A closing endpoint still reports errors of the sends it
                // drains, as asyncio's does; an aborted one reports nothing.
                let error_received = core.with_net(|net, _| {
                    net.datagrams.get(&did).filter(|e| !e.conn_lost).map(|e| e.error_received.clone_ref(py))
                })?;
                if let Some(error_received) = error_received {
                    let exc_obj = os_err(py, err);
                    core.guard_protocol_call(py, error_received.call1(py, (exc_obj,)))?;
                }
            }
            NetEvent::DgramLost { connection_lost, err, close_after, immediate } => {
                let lost = Lost::Datagram { connection_lost, err, close_after };
                if immediate {
                    let res = lost.call(py);
                    core.guard_protocol_call(py, res)?;
                } else {
                    schedule_lost(py, core, lost)?;
                }
            }
            NetEvent::ResumeWriting { tid, resume_writing } => {
                // Owed to the protocol that was paused: not after abort(),
                // and not to a protocol set_protocol() installed since,
                // which starts unpaused. (A closing transport still gets
                // it while its output drains, as in asyncio.)
                let owed = core.with_net(|net, _| match net.transports.get(&tid) {
                    Some(e) if !e.conn_lost => {
                        matches!(&e.proto, ProtoKind::Py(p) if p.resume_writing.is(&resume_writing))
                    }
                    _ => false,
                })?;
                let res = if owed {
                    report_flow_control_error(py, core, tid, resume_writing.call0(py), RESUME_WRITING_FAILED)
                } else {
                    Ok(())
                };
                core.with_net(|net, reactor| finish_after_resume(py, net, reactor.backend_mut(), tid))?;
                core.drain_graveyards(py)?;
                res?;
            }
            NetEvent::Accepted { lid, sock } => {
                enum Action {
                    Factory(Py<PyAny>, Option<Py<PyAny>>),
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
                        ListenerKind::Factory(f) => {
                            Action::Factory(f.clone_ref(py), l.server.as_ref().map(|s| s.clone_ref(py)))
                        }
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
                    Some(Action::Factory(factory, server)) => match factory.call0(py) {
                        Ok(protocol) => {
                            if let Err(e) = wire_stream(py, slf, sock, protocol.into_bound(py), None, server)
                            {
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
                let done: bool =
                    fut.call_method0(intern!(py, "done")).and_then(|v| v.extract()).unwrap_or(true);
                if !done {
                    let msg = if ws {
                        crate::http::ws_message_dict(py, crate::http::WsMsg::Disconnect(1006))?
                    } else {
                        crate::http::disconnect_message(py)?
                    };
                    let _ = fut.call_method1(intern!(py, "set_result"), (msg,));
                }
            }
            NetEvent::HttpDrained { fut } => {
                let fut = fut.bind(py);
                let done: bool =
                    fut.call_method0(intern!(py, "done")).and_then(|v| v.extract()).unwrap_or(true);
                if !done {
                    let _ = fut.call_method1(intern!(py, "set_result"), (py.None(),));
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
                            w.inbox_bytes = w.inbox_bytes.saturating_sub(m.charge());
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
                let done: bool =
                    fut.call_method0(intern!(py, "done")).and_then(|v| v.extract()).unwrap_or(true);
                match (done, msg) {
                    (false, Some(m)) => {
                        let d = crate::http::ws_message_dict(py, m)?;
                        let _ = fut.call_method1(intern!(py, "set_result"), (d,));
                    }
                    (false, None) => {
                        // Spurious wake: park the waiter again -- while there
                        // is still something to wait for. The wake took this
                        // waiter out of recv_waiters, so a teardown between
                        // queueing it and now (an app callback that ran
                        // first, during a graceful shutdown say) could not
                        // release it, and parking it on the gone connection
                        // dropped it unresolved: receive() pending for good,
                        // and its task, which asyncio holds only weakly,
                        // destroyed by the collector while pending. Past EOF
                        // no message can arrive either.
                        let stored = fut.clone().unbind();
                        let unparked = core.with_net(move |net, _| match net.http_conn_mut(tid) {
                            Some(c) if !c.disconnected => {
                                c.recv_waiters.push_back(stored);
                                None
                            }
                            // Handed back so the reference drops out of the
                            // cell (ADR-5).
                            _ => Some(stored),
                        })?;
                        if unparked.is_some() {
                            let msg = crate::http::ws_gone_message(py, core, tid)?;
                            let _ = fut.call_method1(intern!(py, "set_result"), (msg,));
                        }
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
                                w.inbox_bytes = w.inbox_bytes.saturating_add(m.charge());
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
                core.report_net_error(py, "Accept failed", os_err(py, err))?;
            }
            NetEvent::ConnectDone { fut, sock, err } => {
                let fut = fut.bind(py);
                let cancelled: bool =
                    fut.call_method0(intern!(py, "cancelled")).and_then(|v| v.extract()).unwrap_or(true);
                if err != 0 || cancelled {
                    // tcp_connect registered this socket, so detach before
                    // closing (ADR-25). A bare close left the backend
                    // describing a handle value the OS reissues at once --
                    // on IOCP its skip-mode record then passed to the next
                    // socket given that number, and that socket's
                    // synchronous completions were delivered twice.
                    core.with_net(|_net, reactor| reactor.backend_mut().detach_socket(sock))?;
                    netsys::close(sock);
                    if err != 0 && !cancelled {
                        let _ = fut.call_method1(intern!(py, "set_exception"), (os_err(py, err),));
                    }
                } else {
                    let _ = fut.call_method1(intern!(py, "set_result"), (sock as u64,));
                }
            }
            NetEvent::PipeReadDone { fut, buf, bytes, err } => {
                let fut = fut.bind(py);
                let cancelled: bool =
                    fut.call_method0(intern!(py, "cancelled")).and_then(|v| v.extract()).unwrap_or(true);
                if cancelled {
                    // nothing to deliver; the transport already moved on
                } else if err != 0 && err != ERROR_BROKEN_PIPE {
                    let _ = fut.call_method1(intern!(py, "set_exception"), (os_err(py, err),));
                } else {
                    // ERROR_BROKEN_PIPE (the writer closed) is EOF, same
                    // convention as a genuine 0-byte successful read —
                    // matches stdlib's IocpProactor.recv() precisely.
                    let data = PyBytes::new(py, &buf[..bytes as usize]);
                    let _ = fut.call_method1(intern!(py, "set_result"), (data,));
                }
            }
            NetEvent::PipeWriteDone { fut, bytes, err } => {
                let fut = fut.bind(py);
                let cancelled: bool =
                    fut.call_method0(intern!(py, "cancelled")).and_then(|v| v.extract()).unwrap_or(true);
                if cancelled {
                    // nothing to deliver
                } else if err != 0 {
                    let _ = fut.call_method1(intern!(py, "set_exception"), (os_err(py, err),));
                } else {
                    let _ = fut.call_method1(intern!(py, "set_result"), (bytes,));
                }
            }
        }
    }
    Ok(())
}

/// asyncio's `_call_connection_lost`: get_protocol() still answers while
/// connection_lost runs; afterwards the socket is closed and the server
/// forgets the connection. Returns connection_lost's own outcome (or
/// the server's), after the cleanup has run either way.
fn call_connection_lost(py: Python<'_>, lost: ConnLost) -> PyResult<()> {
    let ConnLost { connection_lost, err, immediate: _, transport, protocol, close_after, server } = lost;
    let exc_obj = match err {
        LostReason::Clean => py.None(),
        LostReason::Os(code) => os_err(py, code),
        LostReason::Exc(exc) => exc,
    };
    if let Some(t) = &transport {
        t.get().set_losing(Some(protocol));
    }
    let res = connection_lost.call1(py, (exc_obj,));
    if let Some(t) = &transport {
        // Dropped here, attached and outside the cell. Holding it longer
        // would make protocol -> transport -> protocol a cycle the
        // collector cannot see.
        drop(t.get().set_losing(None));
    }
    drop(close_after); // closes it
    let detached = match server {
        Some(srv) => srv.call_method0(py, intern!(py, "_detach")).map(drop),
        None => Ok(()),
    };
    res?;
    detached
}

/// A torn-down transport's pending connection_lost.
pub(crate) enum Lost {
    Stream(Box<ConnLost>),
    /// The datagram transport's `_connection_lost` and the socket object
    /// to close after it.
    Datagram {
        connection_lost: Py<PyAny>,
        err: Option<u32>,
        close_after: CloseOnDrop,
    },
}

impl Lost {
    fn call(self, py: Python<'_>) -> PyResult<()> {
        match self {
            Lost::Stream(lost) => call_connection_lost(py, *lost),
            Lost::Datagram { connection_lost, err, close_after } => {
                let exc_obj = match err {
                    Some(code) => os_err(py, code),
                    None => py.None(),
                };
                let res = connection_lost.call1(py, (exc_obj,));
                // After the callback, as asyncio's _call_connection_lost.
                drop(close_after);
                res.map(drop)
            }
        }
    }
}

/// A deferred connection_lost, as asyncio defers it: `call_soon` on the
/// facade loop, so it runs after what was already scheduled and before
/// what the caller schedules next. Handed to the loop as the callable
/// below; a failure in connection_lost is then reported the way any
/// failing callback is.
fn schedule_lost(py: Python<'_>, core: &CoreLoop, lost: Lost) -> PyResult<()> {
    let call = Py::new(py, LostCall { lost: Mutex::new(Some(lost)) })?;
    let scheduled =
        core.require_owner().and_then(|owner| owner.call_method1(py, intern!(py, "call_soon"), (&call,)));
    if let Err(e) = scheduled {
        // The loop is closed (asyncio raises the same from _force_close).
        // Nothing will ever run it; dropping it closes the socket now.
        let pending = call.get().lost.lock().unwrap_or_else(|p| p.into_inner()).take();
        drop(pending);
        return Err(e);
    }
    Ok(())
}

/// Deferred connection_lost events a Python-level transport call
/// produced (close(), abort(), a failed write...), scheduled at once:
/// asyncio's `_force_close` calls `call_soon` right there, in order with
/// whatever the caller schedules next. Left in the event queue they
/// would wait for the next tick's dispatch -- a tick late.
pub(crate) fn schedule_pending_lost(py: Python<'_>, core: &CoreLoop) -> PyResult<()> {
    fn deferred(e: &NetEvent) -> bool {
        matches!(e, NetEvent::ConnLost(l) if !l.immediate)
            || matches!(e, NetEvent::DgramLost { immediate: false, .. })
    }
    let lost = core.with_net(|net, _| {
        if !net.events.iter().any(deferred) {
            return Vec::new();
        }
        let mut lost = Vec::new();
        let mut keep = Vec::with_capacity(net.events.len());
        for event in net.events.drain(..) {
            match event {
                NetEvent::ConnLost(l) if !l.immediate => lost.push(Lost::Stream(l)),
                NetEvent::DgramLost { connection_lost, err, close_after, immediate: false } => {
                    lost.push(Lost::Datagram { connection_lost, err, close_after })
                }
                other => keep.push(other),
            }
        }
        net.events = keep;
        lost
    })?;
    for l in lost {
        schedule_lost(py, core, l)?;
    }
    Ok(())
}

/// The callable behind a deferred connection_lost (see `schedule_lost`).
#[pyclass(frozen, module = "cadeloop._core")]
pub struct LostCall {
    lost: Mutex<Option<Lost>>,
}

#[pymethods]
impl LostCall {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let lost = self.lost.lock().unwrap_or_else(|p| p.into_inner()).take();
        match lost {
            Some(lost) => lost.call(py),
            None => Ok(()),
        }
    }

    fn __repr__(&self) -> &'static str {
        "Transport._call_connection_lost"
    }
}

/// Win32 ERROR_BROKEN_PIPE: ReadFile on a pipe whose write end closed can
/// fail with this instead of returning a 0-byte success — stdlib's own
/// proactor treats it identically to EOF (R-051). `pub(crate)` since
/// coreloop.rs's `pipe_read` needs the same constant to apply the
/// identical translation on the *synchronous*-failure path (ReadFile
/// returning FALSE immediately never queues a completion, so it can't
/// be handled here in the poll-dispatch path at all).
pub(crate) const ERROR_BROKEN_PIPE: u32 = 109;

/// The copy loop itself, over any source. Split out so held bytes (which
/// have no pool slot) reach the protocol by exactly the same path as a
/// fresh read.
///
/// SAFETY: `src` must be valid for `len` bytes and must stay valid across
/// the `get_buffer`/`buffer_updated` calls -- either a live pool slot the
/// caller still owns, or a `Vec` the caller keeps alive.
///
/// The error names the callback that failed, in asyncio's wording.
fn copy_into_app_buffer(
    py: Python<'_>,
    get_buffer: &Py<PyAny>,
    buffer_updated: &Py<PyAny>,
    src: *const u8,
    len: usize,
) -> Result<(), (&'static str, PyErr)> {
    let mut copied = 0usize;
    while copied < len {
        // -1, as asyncio passes: "no preference". A buffer of any size
        // works -- a smaller one just takes another round.
        let buf_obj = get_buffer.call1(py, (-1,)).map_err(|e| (GET_BUFFER_FAILED, e))?;
        let mut view: ffi::Py_buffer = unsafe { std::mem::zeroed() };
        let rc = unsafe { ffi::PyObject_GetBuffer(buf_obj.as_ptr(), &mut view, ffi::PyBUF_WRITABLE) };
        if rc != 0 {
            return Err((GET_BUFFER_FAILED, PyErr::fetch(py)));
        }
        let chunk = (view.len as usize).min(len - copied);
        if chunk == 0 {
            unsafe { ffi::PyBuffer_Release(&mut view) };
            return Err((
                GET_BUFFER_FAILED,
                PyRuntimeError::new_err("get_buffer() returned an empty buffer"),
            ));
        }
        unsafe {
            std::ptr::copy_nonoverlapping(src.add(copied), view.buf.cast::<u8>(), chunk);
            ffi::PyBuffer_Release(&mut view);
        }
        copied += chunk;
        buffer_updated.call1(py, (chunk,)).map_err(|e| (BUFFER_UPDATED_FAILED, e))?;
    }
    Ok(())
}

/// Cache protocol callbacks (R-054 bound-method cache). Phase-2 only.
fn cache_proto(py: Python<'_>, protocol: &Bound<'_, PyAny>) -> PyResult<ProtoRefs> {
    let _ = py;
    let is_buffered = protocol.hasattr("get_buffer")? && protocol.hasattr("buffer_updated")?;
    Ok(ProtoRefs {
        protocol: protocol.clone().unbind(),
        data_received: if is_buffered {
            None
        } else {
            Some(protocol.getattr(intern!(py, "data_received"))?.unbind())
        },
        get_buffer: if is_buffered {
            Some(protocol.getattr(intern!(py, "get_buffer"))?.unbind())
        } else {
            None
        },
        buffer_updated: if is_buffered {
            Some(protocol.getattr(intern!(py, "buffer_updated"))?.unbind())
        } else {
            None
        },
        eof_received: protocol.getattr(intern!(py, "eof_received"))?.unbind(),
        connection_lost: protocol.getattr(intern!(py, "connection_lost"))?.unbind(),
        pause_writing: protocol.getattr(intern!(py, "pause_writing"))?.unbind(),
        resume_writing: protocol.getattr(intern!(py, "resume_writing"))?.unbind(),
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
            if path.first() == Some(&0) {
                // Abstract namespace: bytes with the leading NUL, as the
                // socket module reports it -- a string form could not be
                // passed back to socket APIs, and lossy decoding could
                // collide two distinct names.
                return Some(PyBytes::new(py, &path).into_any().unbind());
            }
            // Filesystem path: os.fsdecode semantics (surrogateescape),
            // not lossy UTF-8, so a non-UTF-8 path survives a round trip.
            let obj = unsafe {
                ffi::PyUnicode_DecodeFSDefaultAndSize(path.as_ptr().cast(), path.len() as ffi::Py_ssize_t)
            };
            Some(unsafe { Bound::from_owned_ptr_or_opt(py, obj) }?.unbind())
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
///
/// `owner`: the caller's socket object, when the descriptor is that
/// object's (`sock=`). It stays the owner -- see `TransportEntry::owner`
/// -- and a failure here leaves it with the caller, as asyncio does.
/// `server`: the asyncio Server whose listener accepted the connection;
/// told `_attach()` now and `_detach()` after connection_lost.
pub(crate) fn wire_stream(
    py: Python<'_>,
    slf: &Bound<'_, CoreLoop>,
    sock: RawSocket,
    protocol: Bound<'_, PyAny>,
    owner: Option<Py<PyAny>>,
    server: Option<Py<PyAny>>,
) -> PyResult<Py<Transport>> {
    let core = slf.get();
    let owned = owner.is_none();
    // Everything up to the transport insert is transactional: on any
    // failure a socket with no owner object (it came back from
    // tcp_connect or an accept) must be released here or it leaks.
    // Detach-then-close, never bare close: tcp_connect registered it, and
    // closing a registered socket leaves the backend listing a handle
    // value the OS will reissue, so the next socket to reuse it looks
    // already-associated and its completions are never queued (ADR-25).
    let discard = |core: &CoreLoop| {
        if owned {
            let _ = core.with_net(|_net, reactor| reactor.backend_mut().detach_socket(sock));
            netsys::close(sock);
        }
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
    let connection_made = match protocol.getattr(intern!(py, "connection_made")) {
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
        if owned {
            netsys::close(sock);
        }
        return Err(io_error(e));
    }
    let tid = core.with_net(|net, _| net.next_id())?;
    let transport = Py::new(py, Transport::new(slf.clone().unbind(), tid, peer, name))?;
    if let Some(server) = &server {
        // asyncio counts the connection from the transport's creation.
        core.guard_protocol_call(py, server.call_method0(py, intern!(py, "_attach")))?;
    }
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
                paused_eof: false,
                extra_sock: None,
                extra_fd: None,
                owner,
                server,
                // Kept on the Transport itself (they outlive the entry);
                // the entry's copies serve the native HTTP engine.
                peername: None,
                sockname: None,
            },
        );
        net.fd_users.insert(sock, FdUser::Stream(tid));
    })?;
    // connection_made BEFORE the first recv is posted: no data callback can
    // possibly precede it.
    let made = connection_made.call1((transport.clone_ref(py),));
    core.guard_protocol_call(py, made.map(|b| b.unbind()))?;
    core.with_net(|net, reactor| post_recv(py, net, reactor.backend_mut(), tid))?;
    core.drain_graveyards(py)?;
    schedule_pending_lost(py, core)?;
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
                paused_eof: false,
                extra_sock: None,
                extra_fd: None,
                owner: None,
                server: None,
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
    /// Captured at creation: asyncio's extra info outlives the
    /// connection (connection_lost may still log the peer), and reading
    /// it needs no trip into the state cell.
    peername: Option<Py<PyAny>>,
    sockname: Option<Py<PyAny>>,
    /// `get_extra_info("socket")`: an `asyncio.trsock.TransportSocket`,
    /// built on first ask and kept for the transport's whole life.
    trsock: PyOnceLock<Py<PyAny>>,
    /// The protocol while its connection_lost runs (see `ConnLost`).
    losing: Mutex<Option<Py<PyAny>>>,
    /// write_eof() was called. Kept here, not only on the entry: asyncio
    /// refuses every later write() -- an empty one, and one after the
    /// connection is gone, included -- and the entry does not outlive
    /// the connection.
    eof: AtomicBool,
    /// loop.sendfile() owns the socket: write() raises, as asyncio's does
    /// while its sendfile's empty-waiter is set.
    sendfile: AtomicBool,
    /// The entry's `closing || conn_lost` and `queued_bytes`, mirrored
    /// wherever those change. `is_closing()` and `get_write_buffer_size()`
    /// answer from them when the state is out of reach -- another thread
    /// asking while the loop is parked in its poll, which the state cell
    /// refuses (gil_boundary) -- as asyncio answers both with an attribute
    /// read from any thread.
    closing: AtomicBool,
    queued: AtomicUsize,
}

/// `asyncio.trsock.TransportSocket`, looked up once.
static TRANSPORT_SOCKET: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Wrap `sock` the way asyncio hands out transport sockets. The wrapper
/// refuses what would break the transport -- close(), detach(), and
/// above all setblocking(True): on a plain socket object that switched
/// the shared descriptor to blocking mode, and the engine's next large
/// write then blocked the whole process inside writev with the GIL held.
pub(crate) fn transport_socket(py: Python<'_>, sock: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let cls = TRANSPORT_SOCKET
        .get_or_try_init(py, || py.import("asyncio.trsock")?.getattr("TransportSocket").map(Bound::unbind))?;
    cls.call1(py, (sock,))
}

impl Transport {
    pub(crate) fn new(
        core: Py<CoreLoop>,
        tid: u64,
        peername: Option<Py<PyAny>>,
        sockname: Option<Py<PyAny>>,
    ) -> Self {
        Transport {
            core,
            tid,
            peername,
            sockname,
            trsock: PyOnceLock::new(),
            losing: Mutex::new(None),
            eof: AtomicBool::new(false),
            sendfile: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            queued: AtomicUsize::new(0),
        }
    }

    /// The connection is gone: what `is_closing()` and
    /// `get_write_buffer_size()` answer from now on (see `closing`).
    pub(crate) fn mark_lost(&self) {
        self.closing.store(true, Ordering::Relaxed);
        self.queued.store(0, Ordering::Relaxed);
    }

    fn core_ref<'a>(&'a self, py: Python<'a>) -> &'a CoreLoop {
        self.core.bind(py).get()
    }

    /// Install (or clear) the protocol `get_protocol()` reports once the
    /// entry is gone; returns the previous one for the caller to drop.
    fn set_losing(&self, protocol: Option<Py<PyAny>>) -> Option<Py<PyAny>> {
        let mut slot = self.losing.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::replace(&mut *slot, protocol)
    }

    /// `get_extra_info("socket")`.
    ///
    /// Protocols and libraries reach for this through the standard
    /// transport API to read the address family, inspect socket options,
    /// or set one such as keepalive. A transport built from the caller's
    /// socket (`sock=`) wraps that very object, as asyncio does.
    ///
    /// Otherwise the engine owns the only descriptor, so the wrapper holds
    /// a `dup()` rather than a second owner of the engine's: two owners of
    /// one descriptor means a double close, and by the second one the OS
    /// may have handed that number to an unrelated connection (ADR-25's
    /// hazard, reached from the other side). The duplicate is closed at
    /// teardown (`ConnLost`), which keeps the transport's close final.
    fn transport_socket(&self, py: Python<'_>, core: &CoreLoop) -> PyResult<Option<Py<PyAny>>> {
        if let Some(ts) = self.trsock.get(py) {
            return Ok(Some(ts.clone_ref(py)));
        }
        let found = core.with_net(|net, _| {
            net.transports.get(&self.tid).map(|e| (e.owner.as_ref().map(|o| o.clone_ref(py)), e.socket))
        })?;
        // Gone before anyone asked: there is no socket left to describe.
        let Some((owner, raw)) = found else { return Ok(None) };
        let sock = match owner {
            Some(owner) => owner,
            None => {
                let dup = build_transport_socket(py, raw)?;
                let dup_fd: u64 = dup.call_method0(py, intern!(py, "fileno"))?.extract(py)?;
                let kept = core.with_net(|net, _| match net.transports.get_mut(&self.tid) {
                    Some(e) if e.extra_sock.is_none() => {
                        e.extra_sock = Some(dup.clone_ref(py));
                        // Open as long as the entry is (the TransportSocket
                        // wrapper has no close()), so the number cannot be
                        // reused while it stands for this transport.
                        e.extra_fd = Some(dup_fd as RawSocket);
                        net.fd_users.insert(dup_fd as RawSocket, FdUser::Stream(self.tid));
                        true
                    }
                    _ => false,
                })?;
                if !kept {
                    // Torn down meanwhile: nothing would ever close it.
                    let _ = dup.call_method0(py, intern!(py, "close"));
                    return Ok(None);
                }
                dup
            }
        };
        let ts = transport_socket(py, sock)?;
        let _ = self.trsock.set(py, ts);
        Ok(self.trsock.get(py).map(|t| t.clone_ref(py)))
    }
}

/// A transport's facade object and its protocol, as error context.
type ErrorContext = (Option<Py<PyAny>>, Option<Py<PyAny>>);

/// A validated (non-negative) write-buffer limit as a byte count: an int
/// as is, any other real number floored, saturating at the top (an
/// infinite limit never pauses, as there).
fn byte_limit(v: &Bound<'_, PyAny>) -> PyResult<usize> {
    if let Ok(n) = v.extract::<u64>() {
        return Ok(usize::try_from(n).unwrap_or(usize::MAX));
    }
    let f: f64 = v.extract()?;
    // `as` saturates (and maps NaN to 0, which validation already refused).
    Ok(f.floor() as usize)
}

/// The facade transport and protocol of a Python-protocol transport, for
/// the context asyncio's transport error reports carry.
fn context_objects(py: Python<'_>, core: &CoreLoop, tid: u64) -> PyResult<ErrorContext> {
    core.with_net(|net, _| match net.transports.get(&tid) {
        Some(TransportEntry { proto: ProtoKind::Py(p), pyobj, .. }) => {
            (pyobj.as_ref().map(|t| t.clone_ref(py).into_any()), Some(p.protocol.clone_ref(py)))
        }
        _ => (None, None),
    })
}

/// asyncio's `_fatal_error` for a protocol callback that raised: report
/// it under asyncio's message with the transport and protocol (an OSError
/// is not reported -- asyncio only debug-logs those), then close the
/// connection and hand the exception to connection_lost.
///
/// The receive for the next chunk has already been posted by the time the
/// callback runs, so merely reporting the exception left the connection
/// open and kept feeding bytes to a protocol whose state may be
/// inconsistent. And connection_lost(None) told the protocol the close
/// was orderly, hiding why its connection died.
fn fatal_protocol_error<T>(
    py: Python<'_>,
    core: &CoreLoop,
    tid: u64,
    res: PyResult<T>,
    message: &str,
) -> PyResult<()> {
    let err = match res {
        Ok(_) => return Ok(()),
        Err(e) => e,
    };
    if err.is_instance_of::<PyKeyboardInterrupt>(py) || err.is_instance_of::<PySystemExit>(py) {
        return Err(err);
    }
    let exc = err.into_value(py).into_any();
    if !exc.bind(py).is_instance_of::<PyOSError>() {
        let (transport, protocol) = context_objects(py, core, tid)?;
        core.report_transport_error(py, message, exc.clone_ref(py), transport, protocol)?;
    }
    core.with_net(|net, reactor| teardown_reason(py, net, reactor.backend_mut(), tid, LostReason::Exc(exc)))?;
    core.drain_graveyards(py)
}

/// asyncio's report for a raising pause_writing/resume_writing: the
/// connection stays up, but the handler hears which callback failed and
/// on which transport.
fn report_flow_control_error(
    py: Python<'_>,
    core: &CoreLoop,
    tid: u64,
    res: PyResult<Py<PyAny>>,
    what: &str,
) -> PyResult<()> {
    let err = match res {
        Ok(_) => return Ok(()),
        Err(e) => e,
    };
    if err.is_instance_of::<PyKeyboardInterrupt>(py) || err.is_instance_of::<PySystemExit>(py) {
        return Err(err);
    }
    let (transport, protocol) = context_objects(py, core, tid)?;
    core.report_transport_error(py, what, err.into_value(py).into_any(), transport, protocol)
}

const DATA_RECEIVED_FAILED: &str = "Fatal error: protocol.data_received() call failed.";
const EOF_RECEIVED_FAILED: &str = "Fatal error: protocol.eof_received() call failed.";
const GET_BUFFER_FAILED: &str = "Fatal error: protocol.get_buffer() call failed.";
const BUFFER_UPDATED_FAILED: &str = "Fatal error: protocol.buffer_updated() call failed.";
const PAUSE_WRITING_FAILED: &str = "protocol.pause_writing() failed";
const RESUME_WRITING_FAILED: &str = "protocol.resume_writing() failed";

/// asyncio's write() contract: bytes, bytearray or memoryview, nothing
/// else (an array.array or other buffer exporter is a TypeError there).
fn check_write_type(data: &Bound<'_, PyAny>) -> PyResult<()> {
    let ptr = data.as_ptr();
    let ok = unsafe {
        ffi::PyBytes_Check(ptr) != 0 || ffi::PyByteArray_Check(ptr) != 0 || ffi::PyMemoryView_Check(ptr) != 0
    };
    if ok {
        return Ok(());
    }
    let name = data.get_type().name().map(|n| n.to_string()).unwrap_or_else(|_| "?".to_owned());
    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "data argument must be a bytes-like object, not '{name}'"
    )))
}

/// A plain `socket.socket` over a duplicate of `raw`: the object the
/// engine closes at teardown. Callers see it only through a
/// `TransportSocket` (see `Transport::transport_socket`).
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
        let bytes = data.cast::<PyBytes>().ok();
        if bytes.is_none() {
            check_write_type(&data)?;
        }
        // asyncio's order: the type, then these, then the empty-write and
        // dead-connection shortcuts -- so both of these raise for an empty
        // write too, and write_eof()'s also once the connection is gone.
        if self.eof.load(Ordering::Relaxed) {
            // Half-close MISUSE, not a dead connection: silently
            // succeeding let protocol code believe output was queued when
            // it can never reach the peer -- undetected truncation.
            return Err(PyRuntimeError::new_err("Cannot call write() after write_eof()"));
        }
        if self.sendfile.load(Ordering::Relaxed) {
            // Bytes written now would land in the middle of the file.
            return Err(PyRuntimeError::new_err("unable to write; sendfile is in progress"));
        }
        // R-074 retention: exact bytes -> zero-copy retain; bytearray and
        // memoryview -> copy now.
        let buf: WriteBuf = if let Some(b) = bytes {
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
        core.with_net(|net, reactor| {
            let Some(entry) = net.transports.get_mut(&tid) else {
                net.graveyard_bufs.push(buf);
                dirty = true;
                return; // write after connection_lost: silently dropped
            };
            if entry.conn_lost {
                net.graveyard_bufs.push(buf);
                dirty = true;
                return;
            }
            // A transport that is `closing` but still mapped is draining
            // its queue before the close completes (an idle one is torn
            // down at once by close()). asyncio still sends what is
            // written in that window, and sslproto depends on it: its
            // close_notify can be written from resume_writing after the
            // app closed. Dropping it ended TLS sessions under
            // backpressure with a bare FIN.
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
            pause = maybe_pause(py, net, reactor.backend_mut(), tid);
            if let Some(e) = net.transports.get(&tid) {
                self.queued.store(e.queued_bytes, Ordering::Relaxed);
            }
            dirty |= !net.graveyard_bufs.is_empty() || !net.graveyard_entries.is_empty();
        })?;
        if dirty {
            // Rare paths only (dropped write, >=64KiB flush that tore down).
            core.drain_graveyards(py)?;
            schedule_pending_lost(py, core)?;
        }
        if let Some(pause_writing) = pause {
            report_flow_control_error(py, core, tid, pause_writing.call0(py), PAUSE_WRITING_FAILED)?;
        }
        Ok(())
    }

    fn writelines(&self, py: Python<'_>, list_of_data: Bound<'_, PyAny>) -> PyResult<()> {
        // All or nothing, as asyncio's (which joins the items first): a
        // bad item must not leave the ones before it already queued.
        let items = list_of_data.try_iter()?.collect::<PyResult<Vec<_>>>()?;
        for item in &items {
            if !item.is_instance_of::<PyBytes>() {
                check_write_type(item)?;
            }
        }
        if items.is_empty() {
            // asyncio's writelines is write(b"".join(...)): its checks
            // still apply when there is nothing to write.
            return self.write(py, PyBytes::new(py, b"").into_any());
        }
        for item in items {
            self.write(py, item)?;
        }
        Ok(())
    }

    /// loop.sendfile()'s claim on the socket (see `Transport::sendfile`).
    ///
    /// Exclusive, as asyncio's: a second transfer while one is running is
    /// refused. Overlapping transfers interleaved their chunks on the wire
    /// and parked on the descriptor's one writer watch, where the second
    /// displaced the first -- which then waited for good -- and whichever
    /// finished first dropped the claim for both.
    fn _set_sendfile_active(&self, active: bool) -> PyResult<()> {
        if !active {
            self.sendfile.store(false, Ordering::Relaxed);
        } else if self.sendfile.swap(true, Ordering::Relaxed) {
            return Err(PyRuntimeError::new_err("sendfile is already in progress on this transport"));
        }
        Ok(())
    }

    /// asyncio.constants._SendfileMode.TRY_NATIVE: loop.sendfile() may use
    /// os.sendfile on this transport's socket.
    #[classattr]
    fn _sendfile_compatible(py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mode = py.import("asyncio.constants")?.getattr("_SendfileMode")?.getattr("TRY_NATIVE")?;
        Ok(mode.unbind())
    }

    fn write_eof(&self, py: Python<'_>) -> PyResult<()> {
        let core = self.core_ref(py);
        let tid = self.tid;
        let open = core.with_net(|net, reactor| {
            // asyncio ignores it on a closing transport (`if self._closing
            // or self._eof: return`), and then later writes do not raise.
            let Some(entry) = net.transports.get_mut(&tid) else { return false };
            if entry.closing || entry.conn_lost {
                return false;
            }
            entry.eof_wanted = true;
            maybe_finish_shutdown(py, net, reactor.backend_mut(), tid, false);
            true
        })?;
        if open {
            self.eof.store(true, Ordering::Relaxed);
        }
        core.drain_graveyards(py)?;
        schedule_pending_lost(py, core)
    }

    fn can_write_eof(&self) -> bool {
        true
    }

    fn get_write_buffer_size(&self, py: Python<'_>) -> PyResult<usize> {
        let core = self.core_ref(py);
        let exact = core.try_with_net(|net, _| net.transports.get(&self.tid).map_or(0, |e| e.queued_bytes));
        Ok(exact.unwrap_or_else(|| self.queued.load(Ordering::Relaxed)))
    }

    fn get_write_buffer_limits(&self, py: Python<'_>) -> PyResult<(usize, usize)> {
        let core = self.core_ref(py);
        core.with_net(|net, _| net.transports.get(&self.tid).map_or((0, 0), |e| (e.low_water, e.high_water)))
    }

    #[pyo3(signature = (high=None, low=None))]
    fn set_write_buffer_limits(
        &self,
        py: Python<'_>,
        high: Option<Bound<'_, PyAny>>,
        low: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let core = self.core_ref(py);
        // asyncio's _set_write_buffer_limits, in Python arithmetic so that
        // any real number works as it does there -- a float limit such as
        // `1.5 * 2**20` raised TypeError here and nowhere else. High
        // defaults to 4 * low when only `low` is supplied (not a fixed
        // 64 KiB, which rejected low > 64 KiB outright and gave a
        // different pause threshold from every other loop).
        let high = match (high, &low) {
            (Some(h), _) => h,
            (None, Some(l)) => l.mul(4)?,
            (None, None) => 65536i64.into_pyobject(py)?.into_any(),
        };
        let low = match low {
            Some(l) => l,
            None => high.floor_div(4)?,
        };
        if !(high.ge(&low)? && low.ge(0)?) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "high ({}) must be >= low ({}) must be >= 0",
                high.repr()?,
                low.repr()?
            )));
        }
        // Compared against whole byte counts, `floor(limit)` decides
        // exactly as the limit itself does (uvloop truncates too).
        let (high, low) = (byte_limit(&high)?, byte_limit(&low)?);
        // The queue already has a depth, so the pause decision is re-taken
        // against the new mark, as asyncio's _maybe_pause_protocol does:
        // lowering `high` under an existing backlog pauses now. Raising
        // the limits does not resume here -- asyncio resumes a paused
        // protocol as its buffer drains below the low mark, from the send
        // completion, never from inside this call.
        let pause = core.with_net(|net, reactor| {
            let e = net.transports.get_mut(&self.tid)?;
            e.high_water = high;
            e.low_water = low;
            maybe_pause(py, net, reactor.backend_mut(), self.tid)
        })?;
        core.drain_graveyards(py)?;
        schedule_pending_lost(py, core)?;
        if let Some(pause_writing) = pause {
            report_flow_control_error(py, core, self.tid, pause_writing.call0(py), PAUSE_WRITING_FAILED)?;
        }
        Ok(())
    }

    // ---- read path -----------------------------------------------------

    fn pause_reading(&self, py: Python<'_>) -> PyResult<()> {
        let core = self.core_ref(py);
        let tid = self.tid;
        let _ = tid;
        core.with_net(|net, _reactor| {
            if let Some(entry) = net.transports.get_mut(&tid).filter(|e| !e.closing) {
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
            // A closing transport reads no more (asyncio ignores both calls
            // once close() has run).
            if let Some(entry) = net.transports.get_mut(&tid).filter(|e| !e.closing) {
                if entry.reading_paused {
                    entry.reading_paused = false;
                    // Anything that completed while paused goes out first,
                    // ahead of whatever the fresh recv brings, or the
                    // stream would be reordered. After a held EOF there is
                    // nothing more to read.
                    let eof = flush_paused_recv(net, tid);
                    let still_idle = !eof && net.transports.get(&tid).is_some_and(|e| e.recv_op.is_none());
                    if still_idle {
                        post_recv(py, net, reactor.backend_mut(), tid);
                    }
                }
            }
        })?;
        core.drain_graveyards(py)?;
        schedule_pending_lost(py, core)
    }

    fn is_reading(&self, py: Python<'_>) -> PyResult<bool> {
        let core = self.core_ref(py);
        // asyncio: a closing transport has stopped reading, even while
        // its output is still draining.
        core.with_net(|net, _| {
            net.transports.get(&self.tid).is_some_and(|e| !e.reading_paused && !e.closing && !e.conn_lost)
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
            self.closing.store(true, Ordering::Relaxed);
            if let Some(op) = entry.recv_op.take() {
                // Keep the mapping (see pause_reading): the completion is
                // routed and discarded by the closing/conn_lost checks.
                let _ = backend.cancel(op);
            }
            // Whether output is still buffered decides between closing
            // now and draining first -- and a write() after close() is
            // queued only in the second case (asyncio drops it once the
            // connection is closed). Corked output counts as sent if the
            // kernel takes it now, as it took asyncio's write(), so send
            // it and apply the inline completion before deciding. With
            // `closing` already set, a read completing in that settle is
            // discarded, as asyncio's close() stops reading.
            if send_corked(py, net, backend, tid) {
                net.inline_close = Some(tid);
                settle_inline(py, net, backend);
                net.inline_close = None;
            }
            maybe_finish_shutdown(py, net, backend, tid, false);
        })?;
        core.drain_graveyards(py)?;
        schedule_pending_lost(py, core)
    }

    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        self._force_close(py, None)
    }

    /// Drop the connection now and hand `exc` to `connection_lost`.
    ///
    /// Private in asyncio, but part of the contract its stdlib
    /// `sslproto.SSLProtocol` relies on: a failed handshake, a corrupt
    /// record and both TLS timeouts all end in `transport._force_close(exc)`.
    /// Without it each of those raised AttributeError instead -- the
    /// handshake waiter was never failed, so `create_connection` returned
    /// a `None` transport for an untrusted certificate, and the timeouts
    /// never closed anything.
    #[pyo3(signature = (exc=None))]
    fn _force_close(&self, py: Python<'_>, exc: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        let core = self.core_ref(py);
        let tid = self.tid;
        let reason = match exc {
            Some(e) if !e.is_none() => LostReason::Exc(e.unbind()),
            _ => LostReason::Clean,
        };
        core.with_net(|net, reactor| {
            let backend = reactor.backend_mut();
            // What asyncio discards here is its buffer: output the kernel
            // refused. Corked output had not even been offered yet, and
            // discarding it lost a reply written just before abort(), which
            // every other loop delivers. Its completion is left to the
            // tick (the teardown's reap guard keeps the buffers alive).
            send_corked(py, net, backend, tid);
            teardown_reason(py, net, backend, tid, reason)
        })?;
        core.drain_graveyards(py)?;
        schedule_pending_lost(py, core)
    }

    /// `loop.start_tls()` checks this before upgrading a transport.
    #[classattr]
    fn _start_tls_compatible() -> bool {
        true
    }

    fn is_closing(&self, py: Python<'_>) -> PyResult<bool> {
        let core = self.core_ref(py);
        let exact = core
            .try_with_net(|net, _| net.transports.get(&self.tid).is_none_or(|e| e.closing || e.conn_lost));
        Ok(exact.unwrap_or_else(|| self.closing.load(Ordering::Relaxed)))
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
                // A new protocol starts unpaused: pause_writing and
                // resume_writing come in pairs per protocol, and handing
                // the new one the resume owed to the old one's pause
                // broke start_tls (sslproto's resume_writing asserts it
                // was paused). If the queue is still over the mark, the
                // next write pauses the new protocol properly.
                entry.proto_paused = false;
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
        let live = core.with_net(|net, _| match net.transports.get(&self.tid) {
            Some(TransportEntry { proto: ProtoKind::Py(p), .. }) => Some(p.protocol.clone_ref(py)),
            _ => None,
        })?;
        if let Some(protocol) = live {
            return Ok(protocol);
        }
        let slot = self.losing.lock().unwrap_or_else(|e| e.into_inner());
        Ok(slot.as_ref().map_or_else(|| py.None(), |p| p.clone_ref(py)))
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

    /// asyncio's socket transports always carry `socket`, `sockname` and
    /// `peername` (None when unavailable) and keep them after the
    /// connection is gone; `default` applies to any other name.
    #[pyo3(signature = (name, default=None))]
    fn get_extra_info(&self, py: Python<'_>, name: &str, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let value = match name {
            "peername" => self.peername.as_ref().map(|p| p.clone_ref(py)),
            "sockname" => self.sockname.as_ref().map(|p| p.clone_ref(py)),
            "socket" => self.transport_socket(py, self.core_ref(py))?,
            _ => return Ok(default.unwrap_or_else(|| py.None())),
        };
        Ok(value.unwrap_or_else(|| py.None()))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let closing = self.is_closing(py)?;
        Ok(format!("<cadeloop.Transport tid={} closing={closing}>", self.tid))
    }
}
