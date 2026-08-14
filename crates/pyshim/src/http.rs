//! M2: native HTTP/1.1 + ASGI 3.0 engine (R-080..R-086).
//!
//! Parsing happens in-cell over the recv slot via `cadeloop_core::http`
//! (pure C/Rust — no Python in the critical section); ASGI dispatch runs
//! in phase 2. The response head/body are serialized into the transport's
//! corked write queue as plain Rust bytes — the hot path materializes NO
//! Python objects for the wire format (R-084).
//!
//! Task fast path (R-056): the app coroutine is stepped EAGERLY with
//! `PyIter_Send`. A request that completes without suspending allocates
//! zero asyncio Tasks and zero Futures; `send()` returns a shared
//! singleton completed-awaitable, so steady-state keep-alive requests are
//! allocation-free outside the scope dict itself. Only a coroutine that
//! actually suspends gets a continuation driver (`AppTask`) registered on
//! the awaited future. `Config(eager_tasks=False)` switches to stdlib
//! `asyncio.Task` wrapping instead (§16 escape hatch).
//!
//! Request lifecycle: a request finishes when its app coroutine RETURNS
//! (not when the response completes) — post-response code (Starlette
//! background tasks) runs before the next pipelined request is pumped,
//! matching uvicorn's per-connection serialization, and the pump loop
//! stays iterative (no recursion on pipelined bursts).

use std::collections::VecDeque;

use cadeloop_core::http::{HttpParser, Limits, ParseError, Request};
use pyo3::exceptions::{
    PyBrokenPipeError, PyConnectionResetError, PyKeyboardInterrupt, PyRuntimeError, PySystemExit,
};
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

use crate::coreloop::CoreLoop;
use crate::net::{self, NetState};

// --------------------------------------------------------------------- //
// connection state                                                      //
// --------------------------------------------------------------------- //

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub(crate) enum RespPhase {
    /// Awaiting `http.response.start`.
    Idle,
    /// Start received; head buffered until the first body chunk decides
    /// content-length vs chunked framing (R-084).
    Started,
    /// Streaming body (chunked, caller-framed, or close-delimited).
    Streaming,
    /// Body complete, terminating chunk withheld: the application said it
    /// would send trailers, so the last chunk and the trailer fields have
    /// to go out together (ASGI `http.response.trailers`).
    AwaitingTrailers,
    /// Response fully written.
    Done,
}

pub(crate) struct HttpConn {
    pub(crate) app: Py<PyAny>,
    /// Facade loop: create_future for receive() waiters, create_task for
    /// the non-eager path.
    pub(crate) pyloop: Py<PyAny>,
    /// Lifespan state dict (R-081): shallow-copied into every scope.
    pub(crate) state: Py<PyAny>,
    pub(crate) parser: HttpParser,
    /// Parsed-but-undispatched requests (pipelined; drained sequentially,
    /// R-085).
    pub(crate) pending: VecDeque<Request>,
    /// Bytes retained by `pending`, against MAX_PIPELINE_BYTES.
    pub(crate) pending_bytes: usize,
    /// Reading was stopped because the pipeline budget was spent.
    pub(crate) pipeline_paused: bool,
    /// A request is being processed (its app coroutine has not returned).
    pub(crate) active: bool,
    pub(crate) keep_alive: bool,
    pub(crate) resp: RespPhase,
    pub(crate) resp_head: Vec<u8>,
    /// http.response.start carried a content-length header.
    pub(crate) resp_started_with_length: bool,
    /// Its parsed value, enforced across every body message.
    pub(crate) resp_declared_length: Option<u64>,
    /// Body bytes handed to the wire so far for the active response.
    pub(crate) resp_body_sent: u64,
    /// Streaming without chunked framing (caller CL or close-delimited).
    pub(crate) raw_stream: bool,
    /// Active request is HEAD: body bytes are suppressed on the wire.
    pub(crate) active_head: bool,
    /// Response status forbids a body (RFC 7230 3.3.2: 1xx, 204, 304).
    /// Like `active_head` this suppresses body bytes, and additionally
    /// suppresses the framing headers themselves.
    pub(crate) resp_bodyless: bool,
    /// 205 Reset Content: body bytes suppressed like `resp_bodyless`, but
    /// the empty payload still has to be framed (RFC 7231 6.3.6).
    pub(crate) resp_empty: bool,
    /// The zero-length chunk that introduces the trailer section has
    /// been written, so further trailer messages append fields only.
    pub(crate) trailers_started: bool,
    /// `http.response.start` set `trailers: True`. Only honoured when the
    /// response is genuinely chunked -- HTTP/1.1 carries trailers in the
    /// chunked terminator and nowhere else, so a caller-framed or
    /// close-delimited response has no place to put them.
    pub(crate) resp_trailers: bool,
    /// Active request's HTTP minor version (0 => chunked is unavailable).
    pub(crate) active_minor: u8,
    /// Body of the ACTIVE request, taken by the first `receive()`.
    pub(crate) req_body: Option<Vec<u8>>,
    pub(crate) disconnected: bool,
    /// Pending receive() waiter, resolved with http.disconnect.
    pub(crate) recv_waiter: Option<Py<PyAny>>,
    /// R-084: an ASGI producer parked because the write queue crossed its
    /// high-water mark; resolved when it drains below the low-water mark
    /// (or when the connection dies).
    pub(crate) drain_waiter: Option<Py<PyAny>>,
    /// Cached per-connection ASGI callables (R-083).
    pub(crate) recv_obj: Option<Py<HttpReceive>>,
    pub(crate) send_obj: Option<Py<HttpSend>>,
    /// Suspended continuation (eager path) — kept alive here.
    pub(crate) driver: Option<Py<AppTask>>,
    pub(crate) eager: bool,
    // --- R-080 idle/head timeouts (0 = disabled), driven by http_sweep --
    pub(crate) head_timeout_ns: u64,
    pub(crate) idle_timeout_ns: u64,
    /// Bumped on every recv and request completion; the sweep re-anchors
    /// an Idle connection whose activity moved between sweeps (a fast
    /// request served wholly between two sweeps must not look idle).
    pub(crate) activity: u32,
    /// Sweep bookkeeping: last observed phase (0 idle / 1 head / 2 busy),
    /// activity snapshot, and the anchor timestamp for the current phase.
    pub(crate) sweep_phase: u8,
    pub(crate) sweep_seen: u32,
    pub(crate) sweep_anchor_ns: u64,
    // --- R-140 access log (populated only while a sink is installed) ---
    pub(crate) log_method: &'static str,
    pub(crate) log_target: Vec<u8>,
    pub(crate) log_status: u16,
    pub(crate) log_start_ns: u64,
    // --- R-087 WebSocket mode -----------------------------------------
    /// Socket bytes received after an upgrade head, before (or while) the
    /// WS session runs — fed to the frame parser on accept.
    pub(crate) ws_trailing: Vec<u8>,
    /// Some(_) once the connection is a WebSocket session.
    pub(crate) ws: Option<Box<WsConn>>,
    /// R-059: Some(_) on TLS-terminated connections.
    pub(crate) tls: Option<Box<TlsState>>,
}

/// R-059 native TLS termination: OpenSSL via the interpreter's own _ssl
/// (SSLContext.wrap_bio over a MemoryBIO pair) driven from Rust in
/// phase 2 — full SSLContext fidelity, zero new dependencies, and none
/// of asyncio.sslproto's Python-side state machine. Outbound plaintext
/// stages here; a TlsFlush event encrypts it at the wire boundary.
pub(crate) struct TlsState {
    pub(crate) sslobj: Py<PyAny>,
    pub(crate) inbio: Py<PyAny>,
    pub(crate) outbio: Py<PyAny>,
    pub(crate) handshaking: bool,
    pub(crate) staged: Vec<u8>,
    pub(crate) close_after: bool,
    /// A `close_notify` alert has been produced for this session (R-059).
    /// Until it has, `http_close_after_write` defers the socket shutdown
    /// so the alert can be queued ahead of it.
    pub(crate) shutdown_sent: bool,
}

/// R-087 per-connection WebSocket session state.
pub(crate) struct WsConn {
    pub(crate) rx: cadeloop_core::ws::WsRx,
    /// Assembled inbound ASGI events awaiting receive().
    pub(crate) inbox: std::collections::VecDeque<WsMsg>,
    /// Bytes held by `inbox`, against WS_MAX_INBOX.
    pub(crate) inbox_bytes: usize,
    /// Reading was stopped because the inbox budget was spent.
    pub(crate) inbox_paused: bool,
    /// App accepted the connection (101 sent).
    pub(crate) accepted: bool,
    /// Server sent (or queued) a close frame.
    pub(crate) closing: bool,
    /// websocket.connect delivered to the app.
    pub(crate) connect_sent: bool,
    /// Client's Sec-WebSocket-Key (accept-key derivation at accept time).
    pub(crate) key: Vec<u8>,
    /// Subprotocols the client offered, in offer order. RFC 6455 4.1
    /// allows the server to select one of *these* or none at all;
    /// browsers fail the connection outright on anything else, which
    /// looks like a clean handshake on the server and an instant
    /// disconnect on the client.
    pub(crate) offered: Vec<String>,
    /// Close code the peer sent (or we failed with). Survives teardown in
    /// `NetState.recent_ws_closes` so a receive() that arrives after the
    /// connection is gone still reports what actually happened.
    pub(crate) close_code: Option<u16>,
}

/// R-087: default cap on an assembled inbound message (1009 beyond it).
const WS_MAX_MESSAGE: usize = 1 << 20;

/// Aggregate cap on assembled-but-undelivered WebSocket messages
/// (R-087). WS_MAX_MESSAGE bounds ONE message; this bounds the queue of
/// them, which individually valid sub-limit messages could otherwise
/// grow without end.
const WS_MAX_INBOX: usize = 4 << 20;

pub(crate) enum WsMsg {
    Text(String),
    Binary(Vec<u8>),
    Disconnect(u16),
}

impl WsMsg {
    /// Payload bytes this message holds, for the inbox budget.
    /// What this message costs against `WS_MAX_INBOX`.
    ///
    /// Payload PLUS a fixed per-message charge, so the budget bounds the
    /// queue by depth as well as by bytes. Charging payload alone left a
    /// hole the size of the whole queue: zero-length text and binary
    /// frames are perfectly valid, cost nothing under a byte-only budget,
    /// and an endpoint whose app is slow to call receive() would accept
    /// an unlimited stream of them until the worker died. The charge also
    /// covers what a message really occupies -- a String/Vec header, a
    /// VecDeque slot, and the Python dict receive() builds from it --
    /// none of which is zero for an empty frame.
    ///
    /// Push and pop both go through here; when they did not, the two
    /// sides could drift.
    pub(crate) fn charge(&self) -> usize {
        const PER_MESSAGE: usize = 256;
        let payload = match self {
            WsMsg::Text(s) => s.len(),
            WsMsg::Binary(b) => b.len(),
            WsMsg::Disconnect(_) => 0,
        };
        payload + PER_MESSAGE
    }
}

impl HttpConn {
    pub(crate) fn new(
        app: Py<PyAny>,
        pyloop: Py<PyAny>,
        state: Py<PyAny>,
        limits: Limits,
        eager: bool,
        head_timeout_ns: u64,
        idle_timeout_ns: u64,
    ) -> Self {
        HttpConn {
            app,
            pyloop,
            state,
            parser: HttpParser::new(limits),
            pending: VecDeque::new(),
            pending_bytes: 0,
            pipeline_paused: false,
            active: false,
            keep_alive: true,
            resp: RespPhase::Idle,
            resp_head: Vec::new(),
            resp_started_with_length: false,
            resp_declared_length: None,
            resp_body_sent: 0,
            raw_stream: false,
            active_head: false,
            resp_bodyless: false,
            resp_empty: false,
            resp_trailers: false,
            trailers_started: false,
            active_minor: 1,
            req_body: None,
            disconnected: false,
            recv_waiter: None,
            drain_waiter: None,
            recv_obj: None,
            send_obj: None,
            driver: None,
            eager,
            head_timeout_ns,
            idle_timeout_ns,
            activity: 0,
            sweep_phase: 0,
            sweep_seen: 0,
            sweep_anchor_ns: 0,
            log_method: "",
            log_target: Vec::new(),
            log_status: 0,
            log_start_ns: 0,
            ws_trailing: Vec::new(),
            ws: None,
            tls: None,
        }
    }

    pub(crate) fn take_recv_waiter(&mut self) -> Option<Py<PyAny>> {
        self.recv_waiter.take()
    }

    /// The pipeline queue is at its depth or byte budget.
    pub(crate) fn pipeline_full(&self) -> bool {
        self.pending.len() >= MAX_PIPELINE_DEPTH || self.pending_bytes >= MAX_PIPELINE_BYTES
    }

    /// Drained far enough below the budget to start reading again. The
    /// gap is deliberate: resuming exactly at the limit would toggle the
    /// recv on and off for every single request.
    pub(crate) fn pipeline_drained(&self) -> bool {
        self.pending.len() * 2 < MAX_PIPELINE_DEPTH && self.pending_bytes * 2 < MAX_PIPELINE_BYTES
    }
}

/// R-085 pipeline budget: how many parsed-but-undispatched requests a
/// single connection may hold, and how many bytes they may retain.
///
/// Without these a client can pipeline an unbounded burst behind one slow
/// request -- the receive path keeps reposting reads and every parsed
/// request is retained in full (URL, headers, body) -- and grow
/// `conn.pending` until the worker dies. Reading is paused at the limit
/// and resumed as the queue drains, so the backlog ends up in the peer's
/// send buffer and the kernel's receive window instead of our heap.
pub(crate) const MAX_PIPELINE_DEPTH: usize = 64;
pub(crate) const MAX_PIPELINE_BYTES: usize = 1 << 20;

/// In-cell: feed received bytes into the parser. Parse errors are
/// answered entirely in-cell by the caller (R-086).
pub(crate) struct FeedOutcome {
    /// Phase 2 must run the request pump.
    pub pump: bool,
    /// The pipeline budget is spent: stop reading until it drains.
    pub pause_reading: bool,
    /// A head carried `Expect: 100-continue` and the interim response is
    /// owed to the client (RFC 7231 5.1.1).
    pub owes_continue: bool,
}

pub(crate) fn conn_feed(conn: &mut HttpConn, data: &[u8]) -> Result<FeedOutcome, ParseError> {
    conn.activity = conn.activity.wrapping_add(1);
    if let Some(offset) = conn.parser.feed(data)? {
        // Upgrade head complete (R-087): bytes past it are NOT HTTP —
        // they belong to the upgraded protocol (early client WS frames).
        conn.ws_trailing.extend_from_slice(&data[offset..]);
    }
    while let Some(req) = conn.parser.next_request() {
        conn.pending_bytes += req.queued_size();
        conn.pending.push_back(req);
    }
    Ok(FeedOutcome {
        pump: !conn.pending.is_empty() && !conn.active,
        pause_reading: conn.pipeline_full(),
        owes_continue: conn.parser.take_owes_continue(),
    })
}

/// RFC 7231 5.1.1 interim response. Sent as soon as a head asking for it
/// is parsed, before the body: that is the whole point of the
/// expectation, and a client that does not get it waits out its own
/// continue timeout before sending -- a fixed stall on every upload for
/// the clients that use it, and an indefinite one for the strict ones.
pub(crate) const CONTINUE_RESPONSE: &[u8] = b"HTTP/1.1 100 Continue\r\n\r\n";

/// In-cell: serialized minimal error response (400/413/414/431/500).
pub(crate) fn error_response(err: ParseError) -> Vec<u8> {
    let reason = err.reason;
    format!(
        "HTTP/1.1 {} {}\r\nserver: cadeloop\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        err.status,
        status_text(err.status),
        reason.len(),
        reason
    )
    .into_bytes()
}

fn status_text(code: u16) -> &'static str {
    match code {
        100 => "Continue",
        101 => "Switching Protocols",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        206 => "Partial Content",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        412 => "Precondition Failed",
        413 => "Payload Too Large",
        414 => "URI Too Long",
        415 => "Unsupported Media Type",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "",
    }
}

// --------------------------------------------------------------------- //
// Date header cache (R-084: regenerated once per second)                //
// --------------------------------------------------------------------- //

fn build_date_line(unix_secs: u64) -> Vec<u8> {
    // civil-from-days (Howard Hinnant's algorithm), no chrono dependency.
    let days = (unix_secs / 86400) as i64;
    let secs = unix_secs % 86400;
    let (h, m, s) = (secs / 3600, (secs / 60) % 60, secs % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    let weekday = (days + 4).rem_euclid(7); // 1970-01-01 = Thursday
    const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] =
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    format!(
        "date: {}, {:02} {} {} {:02}:{:02}:{:02} GMT\r\n",
        DAYS[weekday as usize],
        d,
        MONTHS[(month - 1) as usize],
        year,
        h,
        m,
        s
    )
    .into_bytes()
}

/// In-cell date lookup: cached per whole second in NetState (R-084).
pub(crate) fn date_line(net: &mut NetState, unix_secs: u64) -> &[u8] {
    if net.http_date_secs != unix_secs || net.http_date_line.is_empty() {
        net.http_date_secs = unix_secs;
        net.http_date_line = build_date_line(unix_secs);
    }
    &net.http_date_line
}

// --------------------------------------------------------------------- //
// scope construction (R-081, R-082)                                     //
// --------------------------------------------------------------------- //

static ASGI_INFO: PyOnceLock<Py<PyDict>> = PyOnceLock::new();
/// ASGI `scope["extensions"]` -- the engine's declared optional features.
static EXTENSIONS: PyOnceLock<Py<PyDict>> = PyOnceLock::new();
static EMPTY_BYTES: PyOnceLock<Py<PyBytes>> = PyOnceLock::new();
static COMPLETED: PyOnceLock<Py<CompletedAwaitable>> = PyOnceLock::new();

fn method_obj<'py>(py: Python<'py>, m: &'static str) -> Bound<'py, PyString> {
    // R-082: common method strings are interned once.
    match m {
        "GET" => intern!(py, "GET").clone(),
        "POST" => intern!(py, "POST").clone(),
        "PUT" => intern!(py, "PUT").clone(),
        "DELETE" => intern!(py, "DELETE").clone(),
        "HEAD" => intern!(py, "HEAD").clone(),
        "OPTIONS" => intern!(py, "OPTIONS").clone(),
        "PATCH" => intern!(py, "PATCH").clone(),
        _ => PyString::new(py, m),
    }
}

fn percent_decode(path: &[u8]) -> Vec<u8> {
    if !path.contains(&b'%') {
        return path.to_vec();
    }
    let mut out = Vec::with_capacity(path.len());
    let mut i = 0;
    while i < path.len() {
        if path[i] == b'%' && i + 2 < path.len() {
            let hi = (path[i + 1] as char).to_digit(16);
            let lo = (path[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(path[i]);
        i += 1;
    }
    out
}

/// The ASGI two-item form of a transport address (see `build_scope`).
fn asgi_addr(py: Python<'_>, addr: Option<&Py<PyAny>>) -> PyResult<Py<PyAny>> {
    let Some(addr) = addr else { return Ok(py.None()) };
    let bound = addr.bind(py);
    match bound.len() {
        Ok(n) if n > 2 => Ok((bound.get_item(0)?, bound.get_item(1)?).into_pyobject(py)?.into_any().unbind()),
        _ => Ok(addr.clone_ref(py)),
    }
}

fn build_scope<'py>(
    py: Python<'py>,
    req: &Request,
    peer: Option<&Py<PyAny>>,
    local: Option<&Py<PyAny>>,
    state: &Py<PyAny>,
    ws: bool,
    tls: bool,
) -> PyResult<Bound<'py, PyDict>> {
    let scope = PyDict::new(py);
    if ws {
        // R-087: ASGI websocket scope — no method; ws scheme; the
        // client's offered subprotocols.
        scope.set_item(intern!(py, "type"), intern!(py, "websocket"))?;
        let subs = PyList::empty(py);
        for name in offered_subprotocols(req) {
            subs.append(name)?;
        }
        scope.set_item(intern!(py, "subprotocols"), subs)?;
    } else {
        scope.set_item(intern!(py, "type"), intern!(py, "http"))?;
    }
    let asgi = ASGI_INFO.get_or_try_init(py, || -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        d.set_item(intern!(py, "version"), intern!(py, "3.0"))?;
        d.set_item(intern!(py, "spec_version"), intern!(py, "2.3"))?;
        Ok(d.unbind())
    })?;
    scope.set_item(intern!(py, "asgi"), asgi)?;
    if !ws {
        // ASGI extension discovery: an application checks this before
        // setting `trailers: True`, so the engine has to declare it or
        // nothing will ever use the feature.
        let exts = EXTENSIONS.get_or_try_init(py, || -> PyResult<Py<PyDict>> {
            let d = PyDict::new(py);
            d.set_item(intern!(py, "http.response.trailers"), PyDict::new(py))?;
            Ok(d.unbind())
        })?;
        scope.set_item(intern!(py, "extensions"), exts)?;
    }
    scope.set_item(
        intern!(py, "http_version"),
        if req.http_minor == 1 { intern!(py, "1.1") } else { intern!(py, "1.0") },
    )?;
    if !ws {
        scope.set_item(intern!(py, "method"), method_obj(py, req.method))?;
    }
    scope.set_item(
        intern!(py, "scheme"),
        match (ws, tls) {
            (true, true) => intern!(py, "wss"),
            (true, false) => intern!(py, "ws"),
            (false, true) => intern!(py, "https"),
            (false, false) => intern!(py, "http"),
        },
    )?;

    // Split path / query, decode path (R-081: percent-decoded, UTF-8 with
    // latin-1 fallback). RFC 7230 5.3.2: a server MUST accept absolute-form
    // request targets (proxies always send them, and any client may), so
    // strip scheme://authority first — otherwise `path` becomes
    // "http://host/x" and every route misses.
    let (authority, target) = absolute_form_parts(&req.url);
    let q = target.iter().position(|&b| b == b'?');
    let (raw_path, query) = match q {
        Some(i) => (&target[..i], &target[i + 1..]),
        None => (target, &b""[..]),
    };
    // absolute-form with an empty path ("http://host?q") means "/".
    let raw_path = if raw_path.is_empty() { &b"/"[..] } else { raw_path };
    let decoded = percent_decode(raw_path);
    let path_str = match String::from_utf8(decoded) {
        Ok(s) => s,
        Err(e) => e.into_bytes().iter().map(|&b| b as char).collect(),
    };
    scope.set_item(intern!(py, "path"), path_str)?;
    scope.set_item(intern!(py, "raw_path"), PyBytes::new(py, raw_path))?;
    scope.set_item(intern!(py, "query_string"), PyBytes::new(py, query))?;
    scope.set_item(intern!(py, "root_path"), intern!(py, ""))?;

    let headers = PyList::empty(py);
    for (name, value) in &req.headers {
        // RFC 7230 5.4: for an absolute-form target the AUTHORITY is
        // definitive and any Host header is to be ignored. Rewriting it
        // here rather than appending keeps the scope self-consistent --
        // an application that reads Host must not see a value an
        // intermediary already declined to route on.
        let value: &[u8] = match authority {
            Some(a) if name.as_slice() == b"host" => a,
            _ => value,
        };
        headers.append(PyTuple::new(py, [PyBytes::new(py, name), PyBytes::new(py, value)])?)?;
    }
    if let Some(a) = authority {
        if !req.headers.iter().any(|(n, _)| n.as_slice() == b"host") {
            headers.append(PyTuple::new(py, [PyBytes::new(py, b"host"), PyBytes::new(py, a)])?)?;
        }
    }
    scope.set_item(intern!(py, "headers"), headers)?;
    // ASGI defines `client` and `server` as two-item [host, port]
    // iterables. The transport keeps the full socket form, which for IPv6
    // is (host, port, flowinfo, scope_id) -- correct there, since that is
    // what the socket APIs take and return, but handing it to an
    // application breaks the near-universal `host, port = scope["client"]`
    // on IPv6 requests only.
    scope.set_item(intern!(py, "client"), asgi_addr(py, peer)?)?;
    scope.set_item(intern!(py, "server"), asgi_addr(py, local)?)?;
    // R-081: lifespan state, shallow-copied per request (ASGI spec).
    let state_copy = match state.bind(py).cast::<PyDict>() {
        Ok(d) if !d.is_empty() => d.copy()?,
        _ => PyDict::new(py),
    };
    scope.set_item(intern!(py, "state"), state_copy)?;
    Ok(scope)
}

/// R-087: one inbound WS event as its ASGI message dict.
pub(crate) fn ws_message_dict(py: Python<'_>, m: WsMsg) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    match m {
        WsMsg::Text(s) => {
            d.set_item(intern!(py, "type"), intern!(py, "websocket.receive"))?;
            d.set_item(intern!(py, "text"), s)?;
        }
        WsMsg::Binary(b) => {
            d.set_item(intern!(py, "type"), intern!(py, "websocket.receive"))?;
            d.set_item(intern!(py, "bytes"), PyBytes::new(py, &b))?;
        }
        WsMsg::Disconnect(code) => {
            d.set_item(intern!(py, "type"), intern!(py, "websocket.disconnect"))?;
            d.set_item(intern!(py, "code"), code)?;
        }
    }
    Ok(d.into_any().unbind())
}

pub(crate) fn disconnect_message(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item(intern!(py, "type"), intern!(py, "http.disconnect"))?;
    Ok(d.into_any().unbind())
}

/// The disconnect a receive() gets once its connection is already gone.
///
/// `http.disconnect` was returned unconditionally, even for a WebSocket
/// scope -- which happens whenever the peer's close frame is answered and
/// the connection torn down while the app is busy elsewhere rather than
/// parked in receive(). An app that validates the event type rejects it,
/// and one that does not still loses the close code the peer sent.
/// `recent_ws_closes` keeps just enough to answer correctly.
fn gone_message(py: Python<'_>, core: &CoreLoop, tid: u64) -> PyResult<Py<PyAny>> {
    let ws_code = core.with_net(|net, _| net.recent_ws_close(tid))?;
    match ws_code {
        Some(code) => ws_message_dict(py, WsMsg::Disconnect(code)),
        None => disconnect_message(py),
    }
}

// --------------------------------------------------------------------- //
// awaitables                                                            //
// --------------------------------------------------------------------- //

/// Shared, stateless "already done -> None" awaitable: `__next__` raises
/// bare StopIteration immediately. One global instance serves every
/// `send()` (R-083/R-056 zero-allocation path).
#[pyclass(frozen, module = "cadeloop._core")]
pub struct CompletedAwaitable {}

#[pymethods]
impl CompletedAwaitable {
    fn __await__(slf: Py<Self>) -> Py<Self> {
        slf
    }
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }
    fn __next__(&self) -> Option<()> {
        None // StopIteration(None)
    }
}

pub(crate) fn completed(py: Python<'_>) -> &Py<CompletedAwaitable> {
    COMPLETED.get_or_init(py, || Py::new(py, CompletedAwaitable {}).unwrap())
}

/// One-shot "already done -> value" awaitable (receive() results).
#[pyclass(module = "cadeloop._core")]
pub struct ValueAwaitable {
    value: Option<Py<PyAny>>,
}

#[pymethods]
impl ValueAwaitable {
    fn __await__(slf: Py<Self>) -> Py<Self> {
        slf
    }
    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }
    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.value.take() {
            Some(v) => Err(PyErr::from_value(
                pyo3::exceptions::PyStopIteration::new_err((v,)).value(py).clone().into_any(),
            )),
            None => Err(PyRuntimeError::new_err("awaitable already consumed")),
        }
    }
}

fn value_awaitable(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
    Ok(Py::new(py, ValueAwaitable { value: Some(value) })?.into_any())
}

// --------------------------------------------------------------------- //
// receive / send (R-083: per-connection cached callables)               //
// --------------------------------------------------------------------- //

#[pyclass(frozen, module = "cadeloop._core")]
pub struct HttpReceive {
    core: Py<CoreLoop>,
    tid: u64,
}

#[pymethods]
impl HttpReceive {
    fn __call__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let core = self.core.bind(py).get();
        enum R {
            Body(Vec<u8>),
            Disconnect,
            Wait(Py<PyAny>),
            WsConnect,
            Ws(WsMsg),
        }
        let r = core.with_net(|net, _| match net.http_conn_mut(self.tid) {
            None => R::Disconnect,
            Some(conn) if conn.ws.is_some() => {
                let disconnected = conn.disconnected;
                let pyloop = conn.pyloop.clone_ref(py);
                let ws = conn.ws.as_mut().unwrap();
                if !ws.connect_sent {
                    ws.connect_sent = true;
                    R::WsConnect
                } else if let Some(m) = ws.inbox.pop_front() {
                    ws.inbox_bytes = ws.inbox_bytes.saturating_sub(m.charge());
                    R::Ws(m)
                } else if disconnected {
                    R::Ws(WsMsg::Disconnect(1006))
                } else {
                    R::Wait(pyloop)
                }
            }
            Some(conn) => {
                // Buffered body first, even when the peer has already gone:
                // the request was received in full, so the app is entitled
                // to its body. Reporting http.disconnect ahead of it loses
                // data the client did send (common with half-close after a
                // complete POST).
                if let Some(b) = conn.req_body.take() {
                    R::Body(b)
                } else if conn.disconnected {
                    R::Disconnect
                } else {
                    // Body already delivered: resolve on disconnect (or when
                    // this request finishes) — Starlette's disconnect
                    // listeners await here.
                    R::Wait(conn.pyloop.clone_ref(py))
                }
            }
        })?;
        match r {
            R::Disconnect => value_awaitable(py, gone_message(py, core, self.tid)?),
            R::WsConnect => {
                let d = PyDict::new(py);
                d.set_item(intern!(py, "type"), intern!(py, "websocket.connect"))?;
                value_awaitable(py, d.into_any().unbind())
            }
            R::Ws(m) => {
                // A message just left the inbox, so the budget that
                // suppressed reading may have freed up (R-087).
                core.with_net(|net, reactor| {
                    ws_resume_reading(py, net, reactor.backend_mut(), self.tid);
                })?;
                value_awaitable(py, ws_message_dict(py, m)?)
            }
            R::Body(b) => {
                let msg = PyDict::new(py);
                msg.set_item(intern!(py, "type"), intern!(py, "http.request"))?;
                let body_obj = if b.is_empty() {
                    EMPTY_BYTES.get_or_init(py, || PyBytes::new(py, b"").unbind()).bind(py).clone().into_any()
                } else {
                    PyBytes::new(py, &b).into_any()
                };
                msg.set_item(intern!(py, "body"), body_obj)?;
                msg.set_item(intern!(py, "more_body"), false)?;
                value_awaitable(py, msg.into_any().unbind())
            }
            R::Wait(pyloop) => {
                let fut = pyloop.bind(py).call_method0(intern!(py, "create_future"))?;
                let store = fut.clone().unbind();
                let (stored, old) = core.with_net(move |net, _| match net.http_conn_mut(self.tid) {
                    Some(conn) => (true, conn.recv_waiter.replace(store)),
                    None => (false, Some(store)),
                })?;
                if let Some(old) = old {
                    core.with_net(|net, _| net.graveyard_py.push(old))?;
                    core.drain_graveyards(py)?;
                }
                if !stored {
                    let msg = gone_message(py, core, self.tid)?;
                    let _ = fut.call_method1(intern!(py, "set_result"), (msg,));
                }
                Ok(fut.unbind())
            }
        }
    }
}

#[pyclass(frozen, module = "cadeloop._core")]
pub struct HttpSend {
    core: Py<CoreLoop>,
    tid: u64,
}

#[pymethods]
impl HttpSend {
    fn __call__(&self, py: Python<'_>, message: Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        let core = self.core.bind(py).get();
        process_send(py, core, self.tid, &message)?;

        // R-084 write backpressure. Returning an already-completed
        // awaitable unconditionally meant a streaming app whose only
        // suspension point is `await send(...)` could enqueue its entire
        // stream against a slow client -- the loop monopolised, the queue
        // unbounded, and the configured watermarks never applied to the
        // ASGI producer the way they apply to a Python protocol.
        let over = core.with_net(|net, _| {
            net::write_pressure(net, self.tid).map(|(queued, high)| queued > high).unwrap_or(false)
        })?;
        if !over {
            return Ok(completed(py).clone_ref(py).into_any());
        }
        let pyloop = core.with_net(|net, _| net.http_conn_mut(self.tid).map(|c| c.pyloop.clone_ref(py)))?;
        let Some(pyloop) = pyloop else {
            return Ok(completed(py).clone_ref(py).into_any());
        };
        let fut = pyloop.bind(py).call_method0(intern!(py, "create_future"))?;
        let stored = fut.clone().unbind();
        // A previous waiter would only exist if the app called send()
        // concurrently from two tasks, which ASGI forbids; drop it via the
        // graveyard rather than in-cell (ADR-5).
        let displaced = core.with_net(|net, _| net::set_drain_waiter(net, self.tid, stored))?;
        if let Some(old) = displaced {
            core.with_net(|net, _| net.graveyard_py.push(old))?;
            core.drain_graveyards(py)?;
        }
        Ok(fut.unbind())
    }
}

enum SendErr {
    /// Connection torn down: ASGI says raise an OSError subclass.
    Gone,
    /// ASGI protocol violation by the application.
    Proto(&'static str),
}

fn send_err(e: SendErr) -> PyErr {
    match e {
        SendErr::Gone => PyConnectionResetError::new_err("connection closed"),
        SendErr::Proto(m) => PyRuntimeError::new_err(m),
    }
}

/// Handle one ASGI send() message: validate, serialize, enqueue (R-084;
/// R-086: reject response header fields that could forge the message frame.
///
/// An ASGI application reflecting unsanitized request data into a header
/// must not be able to smuggle CRLF and split the response (or inject
/// headers into it). Names are RFC 7230 tokens; values may not carry CR,
/// LF, NUL or other C0 controls. Rejecting is safe here because nothing
/// has been committed yet — the caller turns this into the R-086
/// RuntimeError path, which answers 500 since the response never started.
fn validate_response_header(name: &[u8], value: &[u8]) -> Result<(), String> {
    if name.is_empty() {
        return Err("empty header name".to_string());
    }
    if let Some(&b) = name.iter().find(|&&b| !is_tchar(b)) {
        return Err(format!("invalid byte {b:#04x} in header name"));
    }
    if let Some(&b) = value.iter().find(|&&b| !is_field_vchar(b)) {
        return Err(format!("invalid byte {b:#04x} in header value"));
    }
    Ok(())
}

/// Split a request target into `(authority, origin-form)`.
///
/// absolute-form (`http://host/p?q`) yields `(Some("host"), "/p?q")`;
/// asterisk-form (`*`) and authority-form (`host:port`, CONNECT) have no
/// path and map to `/`; anything already in origin-form passes through
/// with no authority.
///
/// The authority matters, not just the path: RFC 7230 5.4 requires a
/// recipient of an absolute-form request to use THAT authority and ignore
/// any Host header -- and the difference is not academic. An intermediary
/// routing on the request-target while this server's host routing,
/// trusted-host middleware or cache key reads a conflicting header is a
/// routing-confusion primitive, and the header is entirely
/// attacker-controlled.
fn absolute_form_parts(url: &[u8]) -> (Option<&[u8]>, &[u8]) {
    if url.first() == Some(&b'/') {
        return (None, url);
    }
    // scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
    let scheme_len = url
        .iter()
        .position(|&b| !(b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.')))
        .unwrap_or(url.len());
    let is_absolute =
        scheme_len > 0 && url[0].is_ascii_alphabetic() && url.get(scheme_len..scheme_len + 3) == Some(b"://");
    if !is_absolute {
        // asterisk-form or authority-form: no path component.
        return (None, b"/");
    }
    let start = scheme_len + 3;
    match url[start..].iter().position(|&b| matches!(b, b'/' | b'?' | b'#')) {
        Some(i) => (Some(&url[start..start + i]), &url[start + i..]),
        None => (Some(&url[start..]), b"/"),
    }
}

/// RFC 7230 3.3.2 / RFC 7231: responses with these statuses are defined
/// to carry no message body, and no `Content-Length`/`Transfer-Encoding`
/// framing of one.
/// Statuses whose response is terminated by the end of the headers no
/// matter what the headers say (RFC 7230 3.3.3 rule 1): they carry no
/// body *and* need no framing header, so emitting one desynchronises a
/// keep-alive stream.
///
/// 205 is deliberately absent. It also carries no content, but it is not
/// in rule 1's self-framing set -- RFC 7231 6.3.6 requires the server to
/// frame the empty payload explicitly, so it takes the ordinary
/// `content-length: 0` path with its body bytes suppressed.
fn status_forbids_body(status: u16) -> bool {
    (100..200).contains(&status) || status == 204 || status == 304
}

/// 205 Reset Content: no content, but explicitly framed. Without the
/// suppression an application that sends a body on a 205 puts bytes on
/// the wire the client will read as the next response.
fn status_requires_empty_body(status: u16) -> bool {
    status == 205
}

/// Narrower than `status_forbids_body`: 304 carries no body but MAY
/// carry `Content-Length` describing the representation a 200 would have
/// returned (RFC 7232 4.1), so that header is preserved there.
fn status_forbids_content_length(status: u16) -> bool {
    // 205 is here for a different reason than 204: no body reaches the
    // wire, so whatever length the application declared is a promise the
    // response cannot keep. The engine emits `content-length: 0` for it
    // instead of copying that through.
    (100..200).contains(&status) || status == 204 || status == 205
}

/// Subprotocols offered by the client, in offer order.
///
/// One parser for both the `subprotocols` the app sees in its scope and
/// the check on what it selects -- two would eventually disagree about
/// whitespace or empty tokens, and then an accept the app read straight
/// out of its own scope would be rejected.
fn offered_subprotocols(req: &cadeloop_core::http::Request) -> Vec<String> {
    let mut out = Vec::new();
    for (name, value) in &req.headers {
        if name != b"sec-websocket-protocol" {
            continue;
        }
        for part in value.split(|&b| b == b',') {
            // RFC 7230 OWS is SP *or* HTAB, and it is only legal at the
            // token's edges. Filtering spaces anywhere -- which is what
            // this did -- turned `chat,\tsuperchat` into "\tsuperchat",
            // so the app saw a name it could not select: echoing it back
            // emits an invalid negotiation header, and selecting the real
            // one is rejected as unoffered.
            let t: &[u8] = {
                let start = part.iter().position(|b| !matches!(b, b' ' | b'\t')).unwrap_or(part.len());
                let end = part.iter().rposition(|b| !matches!(b, b' ' | b'\t')).map_or(start, |i| i + 1);
                &part[start..end]
            };
            if !t.is_empty() {
                out.push(String::from_utf8_lossy(t).into_owned());
            }
        }
    }
    out
}

/// RFC 7230 `tchar`.
fn is_tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

/// Legal inside a field value: HTAB, SP, VCHAR, and obs-text. Notably NOT
/// CR/LF/NUL (frame forgery) or the remaining C0 controls / DEL.
fn is_field_vchar(b: u8) -> bool {
    b == b'\t' || (0x20..=0x7e).contains(&b) || b >= 0x80
}

/// R-086 protocol violations raise RuntimeError).
fn process_send(py: Python<'_>, core: &CoreLoop, tid: u64, message: &Bound<'_, PyDict>) -> PyResult<()> {
    let mtype: Bound<'_, PyAny> = message
        .get_item(intern!(py, "type"))?
        .ok_or_else(|| PyRuntimeError::new_err("ASGI message missing 'type'"))?;
    let mtype = mtype.cast::<PyString>().map_err(PyErr::from)?;
    let kind = mtype.to_str()?;

    let is_ws = core.with_net(|net, _| net.http_conn_mut(tid).map(|c| c.ws.is_some()).unwrap_or(false))?;
    if is_ws {
        return ws_send(py, core, tid, kind, message);
    }

    if kind == "http.response.start" {
        let status: u16 = message
            .get_item(intern!(py, "status"))?
            .ok_or_else(|| PyRuntimeError::new_err("http.response.start missing 'status'"))?
            .extract()?;
        // A status line is three digits by grammar; anything else puts a
        // malformed start-line on the wire that clients may discard along
        // with the connection.
        if !(100..=999).contains(&status) {
            return Err(PyRuntimeError::new_err(format!(
                "ASGI application sent an invalid response status: {status}"
            )));
        }
        // 1xx is INTERIM: a client keeps waiting for a final response
        // after one. This path allows a single http.response.start and
        // treats its body as the complete response, so emitting 1xx here
        // would leave the client reading the next keep-alive response as
        // this request's final one. Rejected until interim responses are
        // modelled properly.
        if (100..200).contains(&status) {
            return Err(PyRuntimeError::new_err(format!(
                "ASGI application sent informational status {status}; interim \
                 responses are not supported on http.response.start"
            )));
        }
        // Serialize caller headers OUTSIDE the cell (arbitrary Python
        // objects), then commit in-cell.
        // Answer in the version the client spoke. The body path already
        // uses HTTP/1.0-compatible close-delimited framing for a 1.0
        // request, but the status line said 1.1 regardless, and a strict
        // 1.0-only client may reject the higher version outright.
        let minor = core.with_net(|net, _| net.http_conn_mut(tid).map(|c| c.active_minor).unwrap_or(1))?;
        let mut head: Vec<u8> = Vec::with_capacity(256);
        head.extend_from_slice(if minor == 0 { b"HTTP/1.0 " } else { b"HTTP/1.1 " });
        head.extend_from_slice(status.to_string().as_bytes());
        head.push(b' ');
        head.extend_from_slice(status_text(status).as_bytes());
        head.extend_from_slice(b"\r\n");
        let wants_trailers = match message.get_item(intern!(py, "trailers"))? {
            Some(v) => v.is_truthy()?,
            None => false,
        };
        let mut declared_length: Option<u64> = None;
        let mut saw_close = false;
        if let Some(headers) = message.get_item(intern!(py, "headers"))? {
            for item in headers.try_iter()? {
                let pair = item?;
                let name: Vec<u8> = pair.get_item(0)?.extract()?;
                let value: Vec<u8> = pair.get_item(1)?.extract()?;
                if let Err(why) = validate_response_header(&name, &value) {
                    return Err(PyRuntimeError::new_err(format!(
                        "ASGI application sent an invalid response header: {why}"
                    )));
                }
                if name.eq_ignore_ascii_case(b"content-length") && status_forbids_content_length(status) {
                    // HTTP forbids Content-Length on a 204 (and on 1xx).
                    // Suppressing only the GENERATED framing was not
                    // enough: an app-supplied header was still copied
                    // through, and a client honouring it reads the next
                    // keep-alive response's bytes as this one's body.
                    //
                    // NOT 304: RFC 7232 4.1 explicitly PERMITS
                    // Content-Length there, reporting the size of the
                    // representation a 200 would have carried. Stripping
                    // it discards valid cache metadata, which my first
                    // pass at this did.
                    continue;
                }
                if name.eq_ignore_ascii_case(b"content-length") {
                    // Parsed, not merely noted: the body path enforces it,
                    // and a duplicate that disagrees is ambiguous framing.
                    let parsed = std::str::from_utf8(&value).ok().and_then(|v| v.trim().parse::<u64>().ok());
                    let Some(n) = parsed else {
                        return Err(PyRuntimeError::new_err(
                            "ASGI application sent a non-numeric content-length",
                        ));
                    };
                    if declared_length.is_some_and(|prev| prev != n) {
                        return Err(PyRuntimeError::new_err(
                            "ASGI application sent conflicting content-length headers",
                        ));
                    }
                    declared_length = Some(n);
                }
                if name.eq_ignore_ascii_case(b"transfer-encoding") {
                    // Framing is the server's job (R-084). Passing an
                    // app-supplied `chunked` through would either pair it
                    // with our content-length or duplicate our own header,
                    // and the body bytes are not chunk-framed either way.
                    continue;
                }
                if name.eq_ignore_ascii_case(b"connection")
                    && value.to_ascii_lowercase().windows(5).any(|w| w == b"close")
                {
                    saw_close = true;
                }
                if name.eq_ignore_ascii_case(b"date") || name.eq_ignore_ascii_case(b"server") {
                    continue; // ours (R-084)
                }
                head.extend_from_slice(&name);
                head.extend_from_slice(b": ");
                head.extend_from_slice(&value);
                head.extend_from_slice(b"\r\n");
            }
        }
        head.extend_from_slice(b"server: cadeloop\r\n");
        core.with_net(|net, reactor| {
            let unix = {
                let now_ns = reactor.time_cached();
                net.unix_now_secs(now_ns)
            };
            head.extend_from_slice(date_line(net, unix));
            let Some(conn) = net.http_conn_mut(tid) else { return Err(SendErr::Gone) };
            if conn.resp != RespPhase::Idle {
                return Err(SendErr::Proto("http.response.start sent twice"));
            }
            conn.resp_head = std::mem::take(&mut head);
            conn.resp_trailers = wants_trailers;
            conn.resp_started_with_length = declared_length.is_some();
            conn.resp_declared_length = declared_length;
            conn.resp_body_sent = 0;
            conn.resp_bodyless = status_forbids_body(status);
            conn.resp_empty = status_requires_empty_body(status);
            if saw_close {
                conn.keep_alive = false;
            }
            conn.resp = RespPhase::Started;
            conn.log_status = status;
            Ok(())
        })?
        .map_err(send_err)?;
        return Ok(());
    }

    if kind == "http.response.body" {
        let more = match message.get_item(intern!(py, "more_body"))? {
            Some(v) => v.is_truthy()?,
            None => false,
        };
        let body: Vec<u8> = match message.get_item(intern!(py, "body"))? {
            Some(v) if !v.is_none() => v.extract()?,
            _ => Vec::new(),
        };
        core.with_net(|net, reactor| {
            let backend = reactor.backend_mut();
            let Some(conn) = net.http_conn_mut(tid) else { return Err(SendErr::Gone) };
            match conn.resp {
                RespPhase::Idle => Err(SendErr::Proto("http.response.body before http.response.start")),
                RespPhase::Done => Err(SendErr::Proto("http.response.body after the response completed")),
                RespPhase::AwaitingTrailers => Err(SendErr::Proto(
                    "http.response.body after the body completed; send http.response.trailers",
                )),
                RespPhase::Started => {
                    // First body chunk decides the framing (R-084).
                    let mut out = std::mem::take(&mut conn.resp_head);
                    let saw_length = conn.resp_started_with_length;
                    // A bodyless status must not carry body bytes AND must
                    // not carry framing headers: a client reading a 204/304
                    // starts the next response immediately, so a stray
                    // content-length/chunked header (or body) desynchronises
                    // the keep-alive stream.
                    let bodyless = conn.resp_bodyless;
                    let empty = conn.resp_empty;
                    let head_req = conn.active_head || bodyless || empty;
                    let minor = conn.active_minor;
                    // A 205 frames up front even when the application
                    // means to stream: it is not in rule 1's self-framing
                    // set, so the client needs an explicit length, and no
                    // body byte will ever follow to justify chunked.
                    // Trailers ride in the chunked terminator and nowhere
                    // else, so a promised-trailers response has to stream
                    // even when the body arrives in one message.
                    let trailers = conn.resp_trailers && !head_req && minor >= 1 && !saw_length;
                    if (!more && !trailers) || empty {
                        if !saw_length && !bodyless {
                            out.extend_from_slice(b"content-length: ");
                            let framed = if empty { 0 } else { body.len() };
                            out.extend_from_slice(framed.to_string().as_bytes());
                            out.extend_from_slice(b"\r\n");
                        }
                        out.extend_from_slice(b"\r\n");
                        if !head_req {
                            out.extend_from_slice(&body);
                        }
                        conn.resp_body_sent = if head_req { 0 } else { body.len() as u64 };
                        if let Some(e) = length_mismatch(conn, !more) {
                            return Err(e);
                        }
                        if more {
                            conn.raw_stream = true;
                            conn.resp = RespPhase::Streaming;
                        } else {
                            conn.resp = RespPhase::Done;
                        }
                    } else if !trailers && (saw_length || head_req || minor == 0) {
                        // Raw streaming: caller-framed (their CL), a HEAD
                        // response (no body bytes at all), or HTTP/1.0
                        // (close-delimited — chunked needs 1.1).
                        out.extend_from_slice(b"\r\n");
                        if !head_req {
                            out.extend_from_slice(&body);
                        }
                        if minor == 0 && !saw_length && !head_req {
                            // Close-delimited only when the body length is
                            // genuinely implicit; HEAD and bodyless statuses
                            // are self-framing, so the connection survives.
                            conn.keep_alive = false;
                        }
                        conn.resp_body_sent = body.len() as u64;
                        if let Some(e) = length_mismatch(conn, false) {
                            return Err(e);
                        }
                        conn.raw_stream = true;
                        conn.resp = RespPhase::Streaming;
                    } else {
                        out.extend_from_slice(b"transfer-encoding: chunked\r\n\r\n");
                        if !body.is_empty() {
                            push_chunk(&mut out, &body);
                        }
                        conn.raw_stream = false;
                        conn.resp_body_sent = body.len() as u64;
                        conn.resp = if !more {
                            // Body done in one message, but the terminator
                            // waits for the trailers it has to carry.
                            RespPhase::AwaitingTrailers
                        } else {
                            RespPhase::Streaming
                        };
                    }
                    net::http_enqueue(py, net, backend, tid, out);
                    Ok(())
                }
                RespPhase::Streaming => {
                    let raw = conn.raw_stream;
                    let head_req = conn.active_head || conn.resp_bodyless || conn.resp_empty;
                    let mut out = Vec::with_capacity(body.len() + 16);
                    if head_req {
                        // body bytes suppressed
                    } else if raw {
                        out.extend_from_slice(&body);
                    } else if !body.is_empty() {
                        push_chunk(&mut out, &body);
                    }
                    conn.resp_body_sent = conn.resp_body_sent.saturating_add(body.len() as u64);
                    if let Some(e) = length_mismatch(conn, !more) {
                        return Err(e);
                    }
                    if !more {
                        if !raw && !head_req && conn.resp_trailers {
                            // Terminator withheld: it and the trailer
                            // fields are one unit on the wire.
                            conn.resp = RespPhase::AwaitingTrailers;
                        } else {
                            if !raw && !head_req {
                                out.extend_from_slice(b"0\r\n\r\n");
                            }
                            conn.resp = RespPhase::Done;
                        }
                    }
                    net::http_enqueue(py, net, backend, tid, out);
                    Ok(())
                }
            }
        })?
        .map_err(send_err)?;
        return Ok(());
    }

    if kind == "http.response.trailers" {
        let more = match message.get_item(intern!(py, "more_trailers"))? {
            Some(v) => v.is_truthy()?,
            None => false,
        };
        let mut fields: Vec<u8> = Vec::new();
        if let Some(headers) = message.get_item(intern!(py, "headers"))? {
            for item in headers.try_iter()? {
                let pair = item?;
                let name: Vec<u8> = pair.get_item(0)?.extract()?;
                let value: Vec<u8> = pair.get_item(1)?.extract()?;
                // Same guard as the response head: a CR/LF here would
                // forge the end of the message and start a new one.
                if let Err(why) = validate_response_header(&name, &value) {
                    return Err(PyRuntimeError::new_err(format!(
                        "ASGI application sent an invalid trailer: {why}"
                    )));
                }
                // RFC 7230 4.1.2 forbids these in trailers: a recipient
                // that merges trailers into the header set would otherwise
                // have its framing or routing decided after the fact.
                if is_forbidden_trailer(&name) {
                    return Err(PyRuntimeError::new_err(format!(
                        "ASGI application sent a trailer that is not allowed in trailers: {}",
                        String::from_utf8_lossy(&name)
                    )));
                }
                fields.extend_from_slice(&name);
                fields.extend_from_slice(b": ");
                fields.extend_from_slice(&value);
                fields.extend_from_slice(b"\r\n");
            }
        }
        core.with_net(|net, reactor| {
            let backend = reactor.backend_mut();
            let Some(conn) = net.http_conn_mut(tid) else { return Err(SendErr::Gone) };
            if conn.resp != RespPhase::AwaitingTrailers {
                return Err(SendErr::Proto(
                    "http.response.trailers without a preceding body that promised them",
                ));
            }
            let mut out = Vec::with_capacity(fields.len() + 8);
            if conn.trailers_started {
                out.extend_from_slice(&fields);
            } else {
                // The terminating zero-length chunk introduces the
                // trailer section; it was withheld until now.
                conn.trailers_started = true;
                out.extend_from_slice(b"0\r\n");
                out.extend_from_slice(&fields);
            }
            if !more {
                out.extend_from_slice(b"\r\n");
                conn.resp = RespPhase::Done;
            }
            net::http_enqueue(py, net, backend, tid, out);
            Ok(())
        })?
        .map_err(send_err)?;
        return Ok(());
    }

    Err(PyRuntimeError::new_err(format!("unsupported ASGI message type: {mtype}")))
}

/// Fields RFC 7230 4.1.2 forbids in a trailer section.
///
/// Framing, routing and authentication must be decided from the header
/// section; a recipient that merges trailers in would otherwise have them
/// changed after the message was already being processed.
fn is_forbidden_trailer(name: &[u8]) -> bool {
    let mut lower = name.to_ascii_lowercase();
    lower.retain(|b| !b" \t".contains(b));
    matches!(
        lower.as_slice(),
        b"transfer-encoding"
            | b"content-length"
            | b"host"
            | b"cache-control"
            | b"expect"
            | b"max-forwards"
            | b"pragma"
            | b"range"
            | b"te"
            | b"authorization"
            | b"set-cookie"
            | b"content-encoding"
            | b"content-type"
            | b"content-range"
            | b"trailer"
    )
}

/// Enforce a declared `content-length` (R-084). Getting this wrong is not
/// a cosmetic error: on a keep-alive connection a short body makes the
/// client read the start of the NEXT response as this one's body, and a
/// long one makes the surplus look like a new response. Either way the
/// stream is desynchronised, which is the request-smuggling shape.
fn length_mismatch(conn: &HttpConn, final_message: bool) -> Option<SendErr> {
    if conn.active_head || conn.resp_bodyless || conn.resp_empty {
        // No body goes on the wire, and for these the declared length
        // legitimately describes a body that is not being sent: a HEAD
        // response carries the would-be GET length (RFC 7231 4.3.2) and a
        // 304 the would-be 200's (RFC 7232 4.1).
        return None;
    }
    let declared = conn.resp_declared_length?;
    let sent = conn.resp_body_sent;
    if sent > declared {
        return Some(SendErr::Proto("response body exceeded the declared content-length"));
    }
    if final_message && sent < declared {
        return Some(SendErr::Proto("response body was shorter than the declared content-length"));
    }
    None
}

fn push_chunk(out: &mut Vec<u8>, body: &[u8]) {
    out.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(b"\r\n");
}

// --------------------------------------------------------------------- //
// request pump + completion handling (R-056, R-085)                     //
// --------------------------------------------------------------------- //

// --------------------------------------------------------------------- //
// native TLS termination (R-059)                                        //
// --------------------------------------------------------------------- //

static SSL_WANT_READ: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static SSL_WANT_WRITE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn ssl_exc<'py>(
    py: Python<'py>,
    cell: &'static PyOnceLock<Py<PyAny>>,
    name: &str,
) -> PyResult<&'py Py<PyAny>> {
    cell.get_or_try_init(py, || Ok(py.import("ssl")?.getattr(name)?.unbind()))
}

/// Build the per-connection SSLObject over a MemoryBIO pair (phase 2).
pub(crate) fn tls_wrap(py: Python<'_>, ctx: &Py<PyAny>) -> PyResult<TlsState> {
    let ssl_mod = py.import("ssl")?;
    let inbio = ssl_mod.call_method0(intern!(py, "MemoryBIO"))?;
    let outbio = ssl_mod.call_method0(intern!(py, "MemoryBIO"))?;
    let kwargs = PyDict::new(py);
    kwargs.set_item(intern!(py, "server_side"), true)?;
    let sslobj = ctx.bind(py).call_method(intern!(py, "wrap_bio"), (&inbio, &outbio), Some(&kwargs))?;
    Ok(TlsState {
        sslobj: sslobj.unbind(),
        inbio: inbio.unbind(),
        outbio: outbio.unbind(),
        handshaking: true,
        staged: Vec::new(),
        close_after: false,
        shutdown_sent: false,
    })
}

/// Drain the outgoing BIO onto the wire (ciphertext).
fn tls_pump_out(py: Python<'_>, core: &CoreLoop, tid: u64, outbio: &Py<PyAny>) -> PyResult<()> {
    let cipher: Vec<u8> = outbio.call_method0(py, intern!(py, "read"))?.extract(py)?;
    if !cipher.is_empty() {
        core.with_net(|net, reactor| {
            net::http_enqueue_raw(py, net, reactor.backend_mut(), tid, cipher);
        })?;
    }
    Ok(())
}

/// Inbound ciphertext for a TLS connection (phase 2, R-059): feed the
/// The negotiated ALPN protocol, when it is one this engine cannot serve.
///
/// `None` (no ALPN) and the HTTP/1 names are fine; anything else -- in
/// practice `h2`, which a general-purpose `SSLContext` usually advertises
/// -- means the peer will send a protocol the HTTP/1 parser cannot read.
fn alpn_unsupported(py: Python<'_>, sslobj: &Py<PyAny>) -> PyResult<Option<String>> {
    let selected = match sslobj.call_method0(py, intern!(py, "selected_alpn_protocol")) {
        Ok(v) => v,
        // Older/alternative SSL backends may not expose it at all; no
        // ALPN opinion is the same as no ALPN.
        Err(_) => return Ok(None),
    };
    if selected.is_none(py) {
        return Ok(None);
    }
    let name: String = selected.extract(py)?;
    if name == "http/1.1" || name == "http/1.0" {
        Ok(None)
    } else {
        Ok(Some(name))
    }
}

/// incoming BIO, drive the handshake, decrypt, and hand plaintext to the
/// HTTP/WS engine exactly as the plain recv path would.
pub(crate) fn tls_ingest(py: Python<'_>, slf: &Bound<'_, CoreLoop>, tid: u64, data: &[u8]) -> PyResult<()> {
    let core = slf.get();
    let Some((sslobj, inbio, outbio, mut handshaking)) = core.with_net(|net, _| {
        net.http_conn_mut(tid)
            .and_then(|c| c.tls.as_ref())
            .map(|t| (t.sslobj.clone_ref(py), t.inbio.clone_ref(py), t.outbio.clone_ref(py), t.handshaking))
    })?
    else {
        return Ok(());
    };
    inbio.call_method1(py, intern!(py, "write"), (PyBytes::new(py, data),))?;

    let want_read = ssl_exc(py, &SSL_WANT_READ, "SSLWantReadError")?.bind(py);
    let want_write = ssl_exc(py, &SSL_WANT_WRITE, "SSLWantWriteError")?.bind(py);
    if handshaking {
        match sslobj.call_method0(py, intern!(py, "do_handshake")) {
            Ok(_) => {
                if let Some(proto) = alpn_unsupported(py, &sslobj)? {
                    // The engine is HTTP/1-only. Feeding it what an `h2`
                    // client sends -- a connection preface and binary
                    // frames -- fails every request on an otherwise
                    // perfectly good TLS connection, which reads as "the
                    // server is broken" rather than "we do not speak
                    // this". Close instead, and say why.
                    core.report_net_error(
                        py,
                        &format!(
                            "TLS client negotiated ALPN {proto:?}, which cadeloop does not \
                             serve (HTTP/1.1 only) -- closing. Restrict the SSLContext with \
                             set_alpn_protocols([\"http/1.1\"])."
                        ),
                        py.None(),
                    );
                    core.with_net(|net, reactor| {
                        net::teardown_with(py, net, reactor.backend_mut(), tid, None);
                    })?;
                    core.drain_graveyards(py)?;
                    return Ok(());
                }
                handshaking = false;
                let flush = core.with_net(|net, _| {
                    if let Some(t) = net.http_conn_mut(tid).and_then(|c| c.tls.as_mut()) {
                        t.handshaking = false;
                        !t.staged.is_empty()
                    } else {
                        false
                    }
                })?;
                if flush {
                    tls_flush_conn(py, slf, tid)?;
                }
            }
            Err(e)
                if e.matches(py, want_read).unwrap_or(false)
                    || e.matches(py, want_write).unwrap_or(false) =>
            {
                tls_pump_out(py, core, tid, &outbio)?;
                return Ok(());
            }
            Err(_) => {
                // Handshake failure (bad ClientHello, cert rejection,
                // plain HTTP on a TLS port): drop the connection.
                core.with_net(|net, reactor| {
                    net::teardown_with(py, net, reactor.backend_mut(), tid, None);
                })?;
                core.drain_graveyards(py)?;
                return Ok(());
            }
        }
        tls_pump_out(py, core, tid, &outbio)?;
    }
    if handshaking {
        return Ok(());
    }

    // Decrypt everything available, then feed the engine.
    let mut plaintext: Vec<u8> = Vec::new();
    loop {
        match sslobj.call_method1(py, intern!(py, "read"), (65536,)) {
            Ok(obj) => {
                let chunk: Vec<u8> = obj.extract(py)?;
                if chunk.is_empty() {
                    // close_notify: orderly TLS shutdown.
                    core.with_net(|net, reactor| {
                        net::http_close_after_write(py, net, reactor.backend_mut(), tid);
                    })?;
                    break;
                }
                plaintext.extend_from_slice(&chunk);
            }
            // WANT_WRITE is retryable here too, not just on the write
            // path: after the handshake, a TLS state transition (a
            // renegotiation or a key update) can require records to go
            // OUT before decryption can continue. Treating it as fatal
            // tore down a perfectly valid session; the pump below flushes
            // whatever the transition produced and the peer's answer
            // re-enters through tls_ingest.
            Err(e)
                if e.matches(py, want_read).unwrap_or(false)
                    || e.matches(py, want_write).unwrap_or(false) =>
            {
                break
            }
            Err(_) => {
                core.with_net(|net, reactor| {
                    net::teardown_with(py, net, reactor.backend_mut(), tid, None);
                })?;
                core.drain_graveyards(py)?;
                return Ok(());
            }
        }
    }
    tls_pump_out(py, core, tid, &outbio)?;
    // Peer bytes are exactly what a WANT_READ from SSL_write was waiting
    // for, so retry any plaintext `tls_flush_conn` had to re-stage.
    // Without this the response would sit staged forever: the TlsFlush
    // event only fires on the empty -> non-empty transition.
    let restaged = core.with_net(|net, _| {
        net.http_conn_mut(tid).and_then(|c| c.tls.as_ref()).map(|t| !t.staged.is_empty()).unwrap_or(false)
    })?;
    if restaged {
        tls_flush_conn(py, slf, tid)?;
    }
    if plaintext.is_empty() {
        return Ok(());
    }
    let pump = core.with_net(|net, reactor| {
        let backend = reactor.backend_mut();
        let Some(conn) = net.http_conn_mut(tid) else { return false };
        if conn.ws.is_some() {
            ws_ingest(py, net, backend, tid, &plaintext);
            false
        } else {
            match conn_feed(conn, &plaintext) {
                Ok(outcome) => {
                    // TLS reads are driven by the ciphertext recv, which
                    // this path does not repost; flagging the connection
                    // keeps the budget honest so http_resume_reading has
                    // something to release when the queue drains.
                    if outcome.pause_reading {
                        conn.pipeline_paused = true;
                    }
                    if outcome.owes_continue {
                        // Staged as plaintext like any other response
                        // byte; the TlsFlush below encrypts it.
                        net::http_enqueue(py, net, backend, tid, CONTINUE_RESPONSE.to_vec());
                    }
                    outcome.pump
                }
                Err(e) => {
                    // R-086 parity: answer in-cell (staged via TLS), close.
                    let resp = error_response(e);
                    net::http_enqueue(py, net, backend, tid, resp);
                    net::http_close_after_write(py, net, backend, tid);
                    false
                }
            }
        }
    })?;
    if pump {
        pump_requests(py, slf, tid)?;
    }
    Ok(())
}

/// Encrypt staged plaintext and hand ciphertext to the write queue
/// (phase 2, R-059). Consumes a deferred close once the stage is empty.
pub(crate) fn tls_flush_conn(py: Python<'_>, slf: &Bound<'_, CoreLoop>, tid: u64) -> PyResult<()> {
    let core = slf.get();
    let Some((sslobj, outbio, staged, close_after, handshaking)) = core.with_net(|net, _| {
        net.http_conn_mut(tid).and_then(|c| c.tls.as_mut()).map(|t| {
            (
                t.sslobj.clone_ref(py),
                t.outbio.clone_ref(py),
                std::mem::take(&mut t.staged),
                t.close_after,
                t.handshaking,
            )
        })
    })?
    else {
        return Ok(());
    };
    if handshaking {
        // Not ready: re-stage; the handshake completion re-flushes.
        core.with_net(|net, _| {
            if let Some(t) = net.http_conn_mut(tid).and_then(|c| c.tls.as_mut()) {
                let mut back = staged;
                back.extend_from_slice(&t.staged);
                t.staged = back;
            }
        })?;
        return Ok(());
    }
    // Plaintext SSL_write has not consumed yet. Anything left here is
    // re-staged and retried; it must never be dropped, and `close_after`
    // must not fire while it is outstanding, or the response is silently
    // truncated on the wire with no error raised anywhere.
    let mut remaining: &[u8] = &staged;
    while !remaining.is_empty() {
        match sslobj.call_method1(py, intern!(py, "write"), (PyBytes::new(py, remaining),)) {
            Ok(n) => {
                // SSLObject.write returns the plaintext bytes consumed.
                // With an unbounded MemoryBIO that is normally all of them,
                // but the contract permits fewer and dropping the tail
                // would corrupt the body silently.
                let written: usize = n.extract(py).unwrap_or(remaining.len()).min(remaining.len());
                if written == 0 {
                    break; // no progress; wait for the peer
                }
                remaining = &remaining[written..];
                // Drain between chunks so the out BIO cannot be the thing
                // holding up the next write.
                tls_pump_out(py, core, tid, &outbio)?;
            }
            Err(e) => {
                // WANT_READ/WANT_WRITE are retryable states, not failures:
                // a renegotiation needs peer bytes before more plaintext
                // can be encrypted. Tearing the connection down here (and
                // discarding the plaintext taken above) turned an ordinary
                // TLS state transition into a lost response.
                let want_read = ssl_exc(py, &SSL_WANT_READ, "SSLWantReadError")?;
                let want_write = ssl_exc(py, &SSL_WANT_WRITE, "SSLWantWriteError")?;
                if !(e.is_instance(py, want_read.bind(py)) || e.is_instance(py, want_write.bind(py))) {
                    core.with_net(|net, reactor| {
                        net::teardown_with(py, net, reactor.backend_mut(), tid, None);
                    })?;
                    core.drain_graveyards(py)?;
                    return Ok(());
                }
                break;
            }
        }
    }
    // Push out whatever ciphertext this produced: a retryable state
    // usually needs those records to reach the peer before it can make
    // progress, and the peer's reply re-enters through `tls_ingest`,
    // which re-flushes anything still staged.
    tls_pump_out(py, core, tid, &outbio)?;
    if !remaining.is_empty() {
        // Re-stage in front of anything queued while we were outside the
        // cell, preserving stream order, and hold off the close.
        let rest = remaining.to_vec();
        core.with_net(|net, _| {
            if let Some(t) = net.http_conn_mut(tid).and_then(|c| c.tls.as_mut()) {
                let mut back = rest;
                back.extend_from_slice(&t.staged);
                t.staged = back;
            }
        })?;
        return Ok(());
    }
    if close_after {
        tls_send_close_notify(py, core, tid, &sslobj, &outbio)?;
        core.with_net(|net, reactor| {
            if let Some(t) = net.http_conn_mut(tid).and_then(|c| c.tls.as_mut()) {
                t.close_after = false;
            }
            net::http_close_after_write(py, net, reactor.backend_mut(), tid);
        })?;
    }
    Ok(())
}

/// Queue the TLS `close_notify` alert ahead of the socket shutdown
/// (R-059).
///
/// Without it the peer sees the TCP connection end mid-session -- a
/// ragged EOF -- and a strict client reports `SSLEOFError` even though
/// the HTTP response (or WebSocket close) that preceded it was complete
/// and correct.
///
/// `SSLObject.unwrap()` writes the alert into the out BIO and then raises
/// `SSLWantReadError`, because a full TLS shutdown is a two-way exchange
/// and it also wants the peer's alert back. We do not wait for that: a
/// server closing after its own response has nothing further to read, and
/// blocking on the peer would turn a clean close into a hang. Sending
/// ours is what makes the EOF orderly.
fn tls_send_close_notify(
    py: Python<'_>,
    core: &CoreLoop,
    tid: u64,
    sslobj: &Py<PyAny>,
    outbio: &Py<PyAny>,
) -> PyResult<()> {
    let already = core.with_net(|net, _| match net.http_conn_mut(tid).and_then(|c| c.tls.as_mut()) {
        Some(t) if t.shutdown_sent => true,
        Some(t) => {
            t.shutdown_sent = true;
            false
        }
        None => true,
    })?;
    if already {
        return Ok(());
    }
    // Any exception here -- the expected want-read, or a session already
    // torn down by the peer -- means no alert to send and nothing useful
    // left to do, since the connection is closing either way.
    let _ = sslobj.call_method0(py, intern!(py, "unwrap"));
    tls_pump_out(py, core, tid, outbio)
}

/// R-087: websocket.* sends (accept / send / close).
fn ws_send(
    py: Python<'_>,
    core: &CoreLoop,
    tid: u64,
    kind: &str,
    message: &Bound<'_, PyDict>,
) -> PyResult<()> {
    use cadeloop_core::ws;
    match kind {
        "websocket.accept" => {
            let subprotocol: Option<String> = match message.get_item(intern!(py, "subprotocol"))? {
                Some(v) if !v.is_none() => Some(v.extract()?),
                _ => None,
            };
            // The 101 is a real HTTP response head, so it needs the same
            // response-splitting guard as http.response.start: a CR/LF in a
            // subprotocol or extra header would forge frames on the wire.
            // Validate before touching the connection, so a rejected accept
            // leaves `wsc.accepted` clear and the app can retry.
            if let Some(sp) = &subprotocol {
                if let Err(why) = validate_response_header(b"sec-websocket-protocol", sp.as_bytes()) {
                    return Err(PyRuntimeError::new_err(format!(
                        "websocket.accept sent an invalid subprotocol: {why}"
                    )));
                }
                // RFC 6455 4.1: the selected subprotocol must be one the
                // client offered. Sending an unoffered one produced a
                // handshake that looked clean here and made browsers fail
                // the connection immediately -- a disconnect with nothing
                // on the server side to explain it.
                let offered = core.with_net(|net, _| {
                    net.http_conn_mut(tid)
                        .and_then(|c| c.ws.as_ref())
                        .map(|w| w.offered.clone())
                        .unwrap_or_default()
                })?;
                if !offered.iter().any(|o| o == sp) {
                    return Err(PyRuntimeError::new_err(format!(
                        "websocket.accept selected subprotocol {sp:?}, which the client \
                         did not offer (offered: {offered:?})"
                    )));
                }
            }
            let mut extra: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            if let Some(headers) = message.get_item(intern!(py, "headers"))? {
                for item in headers.try_iter()? {
                    let pair = item?;
                    let name: Vec<u8> = pair.get_item(0)?.extract()?;
                    let value: Vec<u8> = pair.get_item(1)?.extract()?;
                    if let Err(why) = validate_response_header(&name, &value) {
                        return Err(PyRuntimeError::new_err(format!(
                            "websocket.accept sent an invalid response header: {why}"
                        )));
                    }
                    // The server generates the handshake fields itself.
                    // Letting an app add its own produces a 101 with
                    // conflicting duplicates, which a compliant client may
                    // reject and intermediaries may resolve differently.
                    // Subprotocol selection has its own dedicated key.
                    const RESERVED: [&[u8]; 4] =
                        [b"sec-websocket-accept", b"sec-websocket-protocol", b"upgrade", b"connection"];
                    if RESERVED.iter().any(|r| name.eq_ignore_ascii_case(r)) {
                        return Err(PyRuntimeError::new_err(format!(
                            "websocket.accept may not set {:?}; it is part of the handshake \
                             the server generates (use 'subprotocol' to select one)",
                            String::from_utf8_lossy(&name)
                        )));
                    }
                    // Extensions are negotiated, not declared, and this
                    // engine negotiates none -- `WsRx` fails any frame
                    // with an RSV bit set (RFC 6455 5.2). Forwarding an
                    // app's `permessage-deflate` therefore told the client
                    // to compress and then killed the connection with 1002
                    // on its first data frame, which reads as a client
                    // bug. Refuse at accept time instead.
                    if name.eq_ignore_ascii_case(b"sec-websocket-extensions") {
                        return Err(PyRuntimeError::new_err(
                            "websocket.accept may not set 'sec-websocket-extensions'; this \
                             engine negotiates no extensions, and a client that honoured the \
                             header would be disconnected for setting a reserved bit",
                        ));
                    }
                    extra.push((name, value));
                }
            }
            let trailing = core
                .with_net(|net, reactor| {
                    let backend = reactor.backend_mut();
                    let Some(conn) = net.http_conn_mut(tid) else { return Err(SendErr::Gone) };
                    let Some(wsc) = conn.ws.as_mut() else { return Err(SendErr::Gone) };
                    if wsc.accepted {
                        return Err(SendErr::Proto("websocket.accept sent twice"));
                    }
                    wsc.accepted = true;
                    let mut head = Vec::with_capacity(192);
                    head.extend_from_slice(b"HTTP/1.1 101 Switching Protocols\r\n");
                    head.extend_from_slice(b"upgrade: websocket\r\nconnection: Upgrade\r\n");
                    head.extend_from_slice(b"sec-websocket-accept: ");
                    head.extend_from_slice(ws::accept_key(&wsc.key).as_bytes());
                    head.extend_from_slice(b"\r\n");
                    if let Some(sp) = &subprotocol {
                        head.extend_from_slice(b"sec-websocket-protocol: ");
                        head.extend_from_slice(sp.as_bytes());
                        head.extend_from_slice(b"\r\n");
                    }
                    for (n, v) in &extra {
                        head.extend_from_slice(n);
                        head.extend_from_slice(b": ");
                        head.extend_from_slice(v);
                        head.extend_from_slice(b"\r\n");
                    }
                    head.extend_from_slice(b"\r\n");
                    conn.log_status = 101;
                    let trailing = std::mem::take(&mut conn.ws_trailing);
                    net::http_enqueue(py, net, backend, tid, head);
                    Ok(trailing)
                })?
                .map_err(send_err)?;
            if !trailing.is_empty() {
                // Client frames that raced ahead of the accept.
                core.with_net(|net, reactor| ws_ingest(py, net, reactor.backend_mut(), tid, &trailing))?;
            }
            core.drain_graveyards(py)?;
            Ok(())
        }
        "websocket.send" => {
            let payload: Vec<u8> = match message.get_item(intern!(py, "text"))? {
                Some(t) if !t.is_none() => {
                    let s: String = t.extract()?;
                    ws::frame(ws::OP_TEXT, s.as_bytes())
                }
                _ => match message.get_item(intern!(py, "bytes"))? {
                    Some(b) if !b.is_none() => {
                        let v: Vec<u8> = b.extract()?;
                        ws::frame(ws::OP_BINARY, &v)
                    }
                    _ => return Err(PyRuntimeError::new_err("websocket.send needs 'text' or 'bytes'")),
                },
            };
            core.with_net(|net, reactor| {
                let backend = reactor.backend_mut();
                let ok = net
                    .http_conn_mut(tid)
                    .and_then(|c| c.ws.as_ref())
                    .map(|w| w.accepted && !w.closing)
                    .unwrap_or(false);
                if ok {
                    net::http_enqueue(py, net, backend, tid, payload);
                    Ok(())
                } else {
                    Err(SendErr::Proto("websocket.send before accept or after close"))
                }
            })?
            .map_err(send_err)?;
            Ok(())
        }
        "websocket.close" => {
            let code: u16 = match message.get_item(intern!(py, "code"))? {
                Some(v) if !v.is_none() => v.extract()?,
                _ => 1000,
            };
            let reason: String = match message.get_item(intern!(py, "reason"))? {
                Some(v) if !v.is_none() => v.extract()?,
                _ => String::new(),
            };
            // RFC 6455 §7.4: 1005/1006/1015 are "not present on the wire"
            // codes and 1004 is reserved, so a peer must treat a close
            // frame carrying one as a protocol error rather than the
            // shutdown the application asked for.
            if !ws::valid_close_code(code) {
                return Err(PyRuntimeError::new_err(format!(
                    "websocket.close sent a reserved or invalid close code: {code}"
                )));
            }
            core.with_net(|net, reactor| {
                let backend = reactor.backend_mut();
                let Some(conn) = net.http_conn_mut(tid) else { return };
                let Some(wsc) = conn.ws.as_mut() else { return };
                if wsc.accepted {
                    if !wsc.closing {
                        wsc.closing = true;
                        let f = ws::close_frame(code, &reason);
                        net::http_enqueue(py, net, backend, tid, f);
                    }
                } else {
                    // ASGI: closing before accept REJECTS the handshake.
                    wsc.accepted = true;
                    wsc.closing = true;
                    conn.log_status = 403;
                    let body = error_response(ParseError { status: 403, reason: "forbidden" });
                    net::http_enqueue(py, net, backend, tid, body);
                }
                net::http_close_after_write(py, net, backend, tid);
            })?;
            core.drain_graveyards(py)?;
            Ok(())
        }
        other => Err(PyRuntimeError::new_err(format!(
            "unsupported ASGI message {other:?} on a websocket connection"
        ))),
    }
}

/// R-087: inbound socket bytes for a WS-mode connection (called from the
/// recv path and for handshake-raced bytes at accept). In-cell.
pub(crate) fn ws_ingest(
    py: Python<'_>,
    net: &mut NetState,
    backend: crate::net::Backend<'_>,
    tid: u64,
    data: &[u8],
) {
    use cadeloop_core::ws::{self, WsEvent};
    let Some(conn) = net.http_conn_mut(tid) else { return };
    let Some(wsc) = conn.ws.as_mut() else { return };
    if !wsc.accepted {
        // Pre-accept client frames wait for the handshake to finish. The
        // app may be doing arbitrary work before it accepts (auth, a
        // database round-trip), and a client is free to stream during that
        // window, so this needs the same cap the accepted path enforces --
        // otherwise one connection can grow it until the worker dies.
        if conn.ws_trailing.len() + data.len() > WS_MAX_MESSAGE {
            // The 101 has NOT been sent yet -- the app has not accepted --
            // so the peer is still parsing an HTTP response. Writing a
            // WebSocket close frame here (which my first version of this
            // cap did) puts raw binary where a status line belongs. Answer
            // in the protocol actually in effect.
            let resp = error_response(ParseError { status: 413, reason: "payload too large" });
            conn.ws_trailing.clear();
            net::http_enqueue(py, net, backend, tid, resp);
            net::http_close_after_write(py, net, backend, tid);
            return;
        }
        conn.ws_trailing.extend_from_slice(data);
        return;
    }
    let mut evs = Vec::new();
    wsc.rx.push(data, &mut evs);
    let mut enqueues: Vec<Vec<u8>> = Vec::new();
    let mut close_after = false;
    let mut wake = false;
    for ev in evs {
        let Some(conn) = net.http_conn_mut(tid) else { return };
        let wsc = conn.ws.as_mut().unwrap();
        match ev {
            WsEvent::Text(s) => {
                let m = WsMsg::Text(s);
                wsc.inbox_bytes += m.charge();
                wsc.inbox.push_back(m);
                wake = true;
            }
            WsEvent::Binary(b) => {
                let m = WsMsg::Binary(b);
                wsc.inbox_bytes += m.charge();
                wsc.inbox.push_back(m);
                wake = true;
            }
            WsEvent::Ping(p) => enqueues.push(ws::frame(ws::OP_PONG, &p)),
            WsEvent::Pong => {}
            WsEvent::Close(code, _reason) => {
                if !wsc.closing {
                    wsc.closing = true;
                    // Echo the close (1005 = no code on the wire -> bare frame).
                    enqueues.push(if code == 1005 {
                        ws::frame(ws::OP_CLOSE, b"")
                    } else {
                        ws::close_frame(code, "")
                    });
                }
                wsc.close_code = Some(code);
                wsc.inbox.push_back(WsMsg::Disconnect(code));
                wake = true;
                close_after = true;
            }
            WsEvent::Fail(code, reason) => {
                if !wsc.closing {
                    wsc.closing = true;
                    enqueues.push(ws::close_frame(code, reason));
                }
                wsc.close_code = Some(code);
                wsc.inbox.push_back(WsMsg::Disconnect(code));
                wake = true;
                close_after = true;
            }
        }
    }
    for buf in enqueues {
        net::http_enqueue(py, net, backend, tid, buf);
    }
    if close_after {
        net::http_close_after_write(py, net, backend, tid);
    }
    if wake {
        if let Some(fut) = net.http_conn_mut(tid).and_then(|c| c.take_recv_waiter()) {
            net.events.push(crate::net::NetEvent::WsWake { tid, fut });
        }
    }
    // R-087: WS_MAX_MESSAGE bounds ONE message; nothing bounded the queue
    // of them. An app that reads slower than the peer writes could be fed
    // an unlimited number of individually valid messages until the worker
    // died. Stop reading here; ws_resume_reading releases it as receive()
    // drains the inbox.
    if let Some(conn) = net.http_conn_mut(tid) {
        if let Some(wsc) = conn.ws.as_mut() {
            if !wsc.inbox_paused && wsc.inbox_bytes >= WS_MAX_INBOX {
                wsc.inbox_paused = true;
                net::pause_reading_for_backpressure(net, tid);
            }
        }
    }
}

/// R-087: re-post the recv the inbox budget suppressed, once receive()
/// has drained it far enough. In-cell.
pub(crate) fn ws_resume_reading(py: Python<'_>, net: &mut NetState, backend: net::Backend<'_>, tid: u64) {
    let resume = match net.http_conn_mut(tid).and_then(|c| c.ws.as_mut()) {
        Some(wsc) if wsc.inbox_paused && wsc.inbox_bytes * 2 < WS_MAX_INBOX => {
            wsc.inbox_paused = false;
            true
        }
        _ => false,
    };
    if resume {
        net::resume_reading_after_backpressure(py, net, backend, tid);
    }
}

/// R-087: the app coroutine returned on a WS connection — close out the
/// session (1000 if still open; 403 if it never accepted).
fn ws_app_done(py: Python<'_>, core: &CoreLoop, tid: u64) -> PyResult<()> {
    use cadeloop_core::ws;
    core.with_net(|net, reactor| {
        let backend = reactor.backend_mut();
        let Some(conn) = net.http_conn_mut(tid) else { return };
        let Some(wsc) = conn.ws.as_mut() else { return };
        if !wsc.accepted {
            wsc.accepted = true;
            wsc.closing = true;
            conn.log_status = 403;
            let body = error_response(ParseError { status: 403, reason: "forbidden" });
            net::http_enqueue(py, net, backend, tid, body);
        } else if !wsc.closing {
            wsc.closing = true;
            net::http_enqueue(py, net, backend, tid, ws::close_frame(1000, ""));
        }
        net::http_close_after_write(py, net, backend, tid);
    })?;
    finish_request(py, core, tid)
}

enum WsVerdict {
    NotWs,
    Ok(Vec<u8>),
    Bad,
}

/// RFC 6455 §4.2.1 server-side handshake validation.
fn ws_validate(req: &Request) -> WsVerdict {
    let mut upgrade_ws = false;
    let mut version_13 = false;
    let mut key: Option<Vec<u8>> = None;
    for (name, value) in &req.headers {
        match name.as_slice() {
            // RFC 7230 6.7: Upgrade is a comma-separated list of
            // protocols, and may be split across repeated fields. Testing
            // the whole value for equality rejected `Upgrade: h2c,
            // websocket` outright, and a later repeated field overwrote an
            // earlier matching one -- so any client or intermediary that
            // offered a second upgrade option got a 400.
            b"upgrade" => {
                upgrade_ws = upgrade_ws
                    || value.split(|&b| b == b',').any(|t| {
                        let start = t.iter().position(|b| !matches!(b, b' ' | b'\t')).unwrap_or(t.len());
                        let end = t.iter().rposition(|b| !matches!(b, b' ' | b'\t')).map_or(start, |i| i + 1);
                        t[start..end].eq_ignore_ascii_case(b"websocket")
                    });
            }
            b"sec-websocket-version" => version_13 = value == b"13",
            b"sec-websocket-key" => key = Some(value.clone()),
            _ => {}
        }
    }
    match (upgrade_ws, version_13, key, req.method) {
        (true, true, Some(k), "GET") if is_ws_key(&k) => WsVerdict::Ok(k),
        _ => WsVerdict::Bad,
    }
}

/// RFC 6455 §4.1: `Sec-WebSocket-Key` is a 16-byte nonce, base64-encoded —
/// so exactly 24 characters ending in "==". Accepting anything else lets a
/// non-WebSocket client (or a cache-poisoning probe) drive the handshake to
/// a 101 whose accept-key it can predict.
fn is_ws_key(k: &[u8]) -> bool {
    k.len() == 24
        && k[22..] == *b"=="
        && k[..22].iter().all(|&b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/')
}

/// Phase 2: start queued requests until the queue is empty or one
/// suspends. ITERATIVE — a pipelined burst of N sync requests runs in one
/// loop, not N nested stacks (AppTask::step never pumps).
pub(crate) fn pump_requests(py: Python<'_>, slf: &Bound<'_, CoreLoop>, tid: u64) -> PyResult<()> {
    let core = slf.get();
    loop {
        // Pop the next request only when no request is active.
        let next = core.with_net(|net, reactor| {
            let logging = net.access_sink.is_some();
            let now_ns = reactor.time_cached();
            let conn = net.http_conn_mut(tid)?;
            if conn.active {
                return None;
            }
            conn.pending.pop_front().map(|mut req| {
                conn.pending_bytes = conn.pending_bytes.saturating_sub(req.queued_size());
                conn.active = true;
                conn.keep_alive = req.keep_alive;
                conn.resp = RespPhase::Idle;
                conn.resp_head = Vec::new();
                conn.resp_started_with_length = false;
                conn.resp_declared_length = None;
                conn.resp_body_sent = 0;
                conn.raw_stream = false;
                conn.active_head = req.method == "HEAD";
                conn.resp_bodyless = false;
                conn.resp_empty = false;
                conn.resp_trailers = false;
                conn.trailers_started = false;
                conn.active_minor = req.http_minor;
                if logging {
                    // R-140: retained only while a sink is installed.
                    conn.log_method = req.method;
                    conn.log_target = req.url.clone();
                    conn.log_status = 0;
                    conn.log_start_ns = now_ns;
                }
                // R-087: a valid WebSocket upgrade flips the connection
                // into WS mode BEFORE dispatch (the scope/receive/send
                // surfaces all branch on it). An upgrade we can't take
                // (missing key, wrong version, non-websocket protocol) is
                // answered 400 below.
                let ws_verdict = if req.upgrade { ws_validate(&req) } else { WsVerdict::NotWs };
                match &ws_verdict {
                    WsVerdict::Ok(key) => {
                        conn.keep_alive = false;
                        conn.ws = Some(Box::new(WsConn {
                            rx: cadeloop_core::ws::WsRx::new(WS_MAX_MESSAGE),
                            inbox: std::collections::VecDeque::new(),
                            inbox_bytes: 0,
                            inbox_paused: false,
                            accepted: false,
                            closing: false,
                            connect_sent: false,
                            key: key.clone(),
                            offered: offered_subprotocols(&req),
                            close_code: None,
                        }));
                    }
                    WsVerdict::Bad => {}
                    WsVerdict::NotWs => {}
                }
                let bad_upgrade = matches!(ws_verdict, WsVerdict::Bad);
                conn.req_body = Some(std::mem::take(&mut req.body));
                (
                    req,
                    conn.app.clone_ref(py),
                    conn.state.clone_ref(py),
                    conn.pyloop.clone_ref(py),
                    conn.eager,
                    conn.ws.is_some(),
                    conn.tls.is_some(),
                    bad_upgrade,
                )
            })
        })?;
        let Some((req, app, state, pyloop, eager, is_ws, is_tls, bad_upgrade)) = next else {
            return Ok(());
        };
        if bad_upgrade {
            // R-087: malformed upgrade — answered in-cell, connection closed.
            core.with_net(|net, reactor| {
                let backend = reactor.backend_mut();
                if let Some(c) = net.http_conn_mut(tid) {
                    c.active = false;
                    c.keep_alive = false;
                }
                let body = error_response(ParseError { status: 400, reason: "bad websocket upgrade" });
                net::http_enqueue(py, net, backend, tid, body);
                net::http_close_after_write(py, net, backend, tid);
            })?;
            return Ok(());
        }

        let (peer, local) = core.with_net(|net, _| net.peer_local(py, tid))?;
        let scope = build_scope(py, &req, peer.as_ref(), local.as_ref(), &state, is_ws, is_tls)?;
        drop(req);

        // Per-connection cached receive/send callables (R-083).
        let cached = core.with_net(|net, _| {
            let conn = net.http_conn_mut(tid)?;
            Some((
                conn.recv_obj.as_ref().map(|o| o.clone_ref(py)),
                conn.send_obj.as_ref().map(|o| o.clone_ref(py)),
            ))
        })?;
        let Some((recv_obj, send_obj)) = cached else { return Ok(()) };
        let recv_obj = match recv_obj {
            Some(o) => o,
            None => {
                let o = Py::new(py, HttpReceive { core: slf.clone().unbind(), tid })?;
                core.with_net(|net, _| {
                    if let Some(c) = net.http_conn_mut(tid) {
                        c.recv_obj = Some(o.clone_ref(py));
                    }
                })?;
                o
            }
        };
        let send_obj = match send_obj {
            Some(o) => o,
            None => {
                let o = Py::new(py, HttpSend { core: slf.clone().unbind(), tid })?;
                core.with_net(|net, _| {
                    if let Some(c) = net.http_conn_mut(tid) {
                        c.send_obj = Some(o.clone_ref(py));
                    }
                })?;
                o
            }
        };

        let coro = match app.bind(py).call1((scope, recv_obj, send_obj)) {
            Ok(c) => c,
            Err(e) => {
                app_failure(py, core, tid, e)?;
                continue;
            }
        };

        if eager {
            // R-056: step inline; only a suspension allocates a driver.
            let task = AppTask::spawn(py, slf, tid, coro.unbind(), pyloop)?;
            let initial = AppTask::step(task.bind(py), py, None)?;
            match initial {
                StepOutcome::Suspended => {
                    core.with_net(|net, _| {
                        if let Some(c) = net.http_conn_mut(tid) {
                            c.driver = Some(task.clone_ref(py));
                        }
                    })?;
                    return Ok(()); // resumes via future callbacks
                }
                StepOutcome::Finished => on_coro_finished(py, core, tid)?,
                StepOutcome::Failed => {} // app_failure already ran in step
            }
        } else {
            // §16 escape hatch: full stdlib Task semantics
            // (asyncio.current_task() is a real Task; contextvars isolated).
            //
            // Spawning can fail on the application's account -- an ASGI
            // callable that returns a non-coroutine, or an installed task
            // factory that rejects it. Propagating that out of
            // pump_requests took the error through the native tick and
            // stopped the whole worker's loop, where the eager branch
            // turns the same per-request mistake into a 500 and keeps
            // serving everyone else. Route it the same way.
            let spawned = (|| -> PyResult<()> {
                let task = pyloop.bind(py).call_method1(intern!(py, "create_task"), (coro,))?;
                let cb = Py::new(py, HttpTaskDone { core: slf.clone().unbind(), tid })?;
                task.call_method1(intern!(py, "add_done_callback"), (cb,))?;
                Ok(())
            })();
            if let Err(e) = spawned {
                app_failure(py, core, tid, e)?;
            }
            return Ok(());
        }
    }
}

/// The app coroutine returned: verify the response completed (R-086) and
/// finish the request cycle.
fn on_coro_finished(py: Python<'_>, core: &CoreLoop, tid: u64) -> PyResult<()> {
    let (complete, is_ws, disconnected) = core.with_net(|net, _| {
        net.http_conn_mut(tid)
            .map(|c| (c.resp == RespPhase::Done, c.ws.is_some(), c.disconnected))
            .unwrap_or((true, false, false))
    })?;
    if is_ws {
        return ws_app_done(py, core, tid);
    }
    if complete || disconnected {
        // A disconnect-triggered early return (StreamingResponse/SSE:
        // the peer went away mid-stream, so the app coroutine returns
        // without ever sending the final chunk) is a normal ASGI
        // outcome, not an application bug — real uvicorn logs nothing
        // for the identical scenario. Mirrors how app_failure already
        // treats ConnectionResetError/BrokenPipeError as benign, just
        // reached from the "returned normally" side instead of a raise.
        finish_request(py, core, tid)
    } else {
        app_failure(
            py,
            core,
            tid,
            PyRuntimeError::new_err("ASGI application returned without completing the response"),
        )
    }
}

/// App raised (R-086): 500 if the response never started, teardown if it
/// died mid-response. Client disconnects are not reported as app errors.
pub(crate) fn app_failure(py: Python<'_>, core: &CoreLoop, tid: u64, err: PyErr) -> PyResult<()> {
    if err.is_instance_of::<PyKeyboardInterrupt>(py) || err.is_instance_of::<PySystemExit>(py) {
        return Err(err);
    }
    let disconnect =
        err.is_instance_of::<PyConnectionResetError>(py) || err.is_instance_of::<PyBrokenPipeError>(py);
    if !disconnect {
        core.report_net_error(py, "Exception in ASGI application", err.into_value(py).into_any());
    }
    let is_ws = core.with_net(|net, _| net.http_conn_mut(tid).map(|c| c.ws.is_some()).unwrap_or(false))?;
    if is_ws {
        // R-087: app died mid-session -> 1011 (or reject if never accepted).
        use cadeloop_core::ws;
        core.with_net(|net, reactor| {
            let backend = reactor.backend_mut();
            let Some(conn) = net.http_conn_mut(tid) else { return };
            let Some(wsc) = conn.ws.as_mut() else { return };
            if !wsc.accepted {
                wsc.accepted = true;
                wsc.closing = true;
                conn.log_status = 500;
                let body = error_response(ParseError { status: 500, reason: "internal server error" });
                net::http_enqueue(py, net, backend, tid, body);
            } else if !wsc.closing {
                wsc.closing = true;
                net::http_enqueue(py, net, backend, tid, ws::close_frame(1011, ""));
            }
            net::http_close_after_write(py, net, backend, tid);
        })?;
        return finish_request(py, core, tid);
    }
    let phase = core.with_net(|net, _| net.http_conn_mut(tid).map(|c| c.resp))?;
    match phase {
        None => Ok(()), // connection already gone
        Some(RespPhase::Done) => {
            // Post-response failure (background task): the response is
            // intact — just finish the cycle.
            finish_request(py, core, tid)
        }
        Some(RespPhase::Idle) => {
            let log = core.with_net(|net, reactor| {
                let now_ns = reactor.time_cached();
                let backend = reactor.backend_mut();
                if let Some(c) = net.http_conn_mut(tid) {
                    c.resp = RespPhase::Done;
                    c.keep_alive = false;
                    c.log_status = 500;
                }
                let log = take_access_record(py, net, tid, now_ns);
                let body = error_response(ParseError { status: 500, reason: "internal server error" });
                net::http_enqueue(py, net, backend, tid, body);
                net::http_close_after_write(py, net, backend, tid);
                log
            })?;
            emit_access_record(py, log);
            core.drain_graveyards(py)
        }
        Some(_) => {
            // Mid-response: the framing cannot be repaired — hard close.
            core.with_net(|net, reactor| net::teardown_with(py, net, reactor.backend_mut(), tid, None))?;
            core.drain_graveyards(py)
        }
    }
}

/// Reset per-request state; close (`connection: close` / disconnect) or
/// continue keep-alive. The caller pumps pipelined requests (R-085).
/// R-140: pull the completed request's access record + sink out of the
/// state cell (None while logging is disabled). Must be called BEFORE
/// the record fields are reset for the next request.
type AccessRecord = (Py<PyAny>, Option<Py<PyAny>>, &'static str, Vec<u8>, u16, f64);

fn take_access_record(
    py: Python<'_>,
    net: &mut crate::net::NetState,
    tid: u64,
    now_ns: u64,
) -> Option<AccessRecord> {
    net.access_sink.as_ref()?;
    let conn = net.http_conn_mut(tid)?;
    if conn.log_method.is_empty() {
        return None;
    }
    let method = conn.log_method;
    conn.log_method = "";
    let target = std::mem::take(&mut conn.log_target);
    let status = conn.log_status;
    let dur_ms = now_ns.saturating_sub(conn.log_start_ns) as f64 / 1e6;
    let (peer, _local) = net.peer_local(py, tid);
    Some((net.access_sink.as_ref().unwrap().clone_ref(py), peer, method, target, status, dur_ms))
}

/// Emit outside the state cell: the sink is arbitrary Python (R-140).
fn emit_access_record(py: Python<'_>, rec: Option<AccessRecord>) {
    if let Some((sink, peer, method, target, status, dur_ms)) = rec {
        let target = pyo3::types::PyBytes::new(py, &target);
        if let Err(e) = sink.call1(py, (peer, method, target, status, dur_ms)) {
            e.write_unraisable(py, None);
        }
    }
}

pub(crate) fn finish_request(py: Python<'_>, core: &CoreLoop, tid: u64) -> PyResult<()> {
    let (waiter, log) = core.with_net(|net, reactor| {
        let now_ns = reactor.time_cached();
        let backend = reactor.backend_mut();
        let log = take_access_record(py, net, tid, now_ns);
        let (waiter, driver, close) = {
            let Some(conn) = net.http_conn_mut(tid) else { return (None, None) };
            conn.active = false;
            conn.req_body = None;
            conn.activity = conn.activity.wrapping_add(1);
            (conn.take_recv_waiter(), conn.driver.take(), !conn.keep_alive || conn.disconnected)
        };
        if let Some(d) = driver {
            net.graveyard_py.push(d.into_any());
        }
        if close {
            net::http_close_after_write(py, net, backend, tid);
        } else {
            // R-085: a request just left the pipeline queue, so the budget
            // that suppressed reading may have freed up.
            net::http_resume_reading(py, net, backend, tid);
        }
        (waiter, log)
    })?;
    emit_access_record(py, log);
    core.drain_graveyards(py)?;
    if let Some(fut) = waiter {
        let fut = fut.bind(py);
        let done: bool = fut.call_method0(intern!(py, "done")).and_then(|v| v.extract()).unwrap_or(true);
        if !done {
            let _ = fut.call_method1(intern!(py, "set_result"), (disconnect_message(py)?,));
        }
    }
    Ok(())
}

/// Drop the stored driver reference (failure paths where finish_request
/// did not run).
fn drop_driver(py: Python<'_>, core: &CoreLoop, tid: u64) -> PyResult<()> {
    let dropped = core.with_net(|net, _| {
        let d = net.http_conn_mut(tid).and_then(|c| c.driver.take());
        if let Some(d) = d {
            net.graveyard_py.push(d.into_any());
            true
        } else {
            false
        }
    })?;
    if dropped {
        core.drain_graveyards(py)?;
    }
    Ok(())
}

// --------------------------------------------------------------------- //
// AppTask: the eager continuation driver                                //
// --------------------------------------------------------------------- //

#[derive(Debug)]
pub(crate) enum StepOutcome {
    Suspended,
    Finished,
    /// The coroutine raised; `app_failure` already handled it.
    Failed,
}

/// `asyncio.tasks._enter_task` / `_leave_task` (C-accelerated when
/// available): registers the AppTask as `asyncio.current_task()` while it
/// steps, which anyio/Starlette task groups rely on (they weakref and
/// interrogate the host task).
static ENTER_TASK: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static LEAVE_TASK: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static CANCELLED_ERROR: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn cancelled_error(py: Python<'_>) -> PyErr {
    let cls = CANCELLED_ERROR.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py.import("asyncio")?.getattr("CancelledError")?.unbind())
    });
    match cls {
        Ok(cls) => match cls.bind(py).call0() {
            Ok(exc) => PyErr::from_value(exc),
            Err(e) => e,
        },
        Err(e) => e,
    }
}

fn task_hook<'py>(
    py: Python<'py>,
    cell: &'static PyOnceLock<Py<PyAny>>,
    name: &str,
) -> PyResult<&'py Py<PyAny>> {
    cell.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        let m = py.import("_asyncio").or_else(|_| py.import("asyncio.tasks"))?;
        Ok(m.getattr(name)?.unbind())
    })
}

/// Lightweight Task stand-in driving eagerly-stepped app coroutines
/// (R-056). While stepping it registers as `asyncio.current_task()` and
/// exposes the Task surface anyio-based frameworks touch (weakref,
/// cancel/uncancel, get_loop/get_name); it is still not a full
/// `asyncio.Task` — switch `eager_tasks` off (§16) for libraries that
/// need complete Task semantics (e.g. task introspection, eager cancel
/// counts).
#[pyclass(weakref, module = "cadeloop._core")]
pub struct AppTask {
    core: Py<CoreLoop>,
    tid: u64,
    coro: Py<PyAny>,
    pyloop: Py<PyAny>,
    finished: bool,
    must_cancel: bool,
    /// The future this task is suspended on (cancel() forwards to it,
    /// mirroring Task.cancel()).
    waiting_on: Option<Py<PyAny>>,
    /// Captured at spawn() time (contextvars.Context.copy(), mirroring
    /// real asyncio.Task.__init__) and entered/exited around every
    /// coroutine step in step_inner — without this, contextvars set
    /// during one request are visible to unrelated concurrent or later
    /// requests on the same worker (handles.rs::run_handle already
    /// applies this discipline to plain callbacks; AppTask's raw
    /// PyIter_Send stepping never did).
    context: Py<PyAny>,
    /// Future/Task done-callback protocol. anyio's asyncio backend
    /// resolves the "root task" for a thread-offload
    /// (anyio.to_thread.run_sync, which FastAPI's plain `def` routes and
    /// sync Depends() go through) to whatever `asyncio.current_task()`
    /// returns — the AppTask itself under the eager engine — then reads
    /// `root_task._loop` and calls `root_task.add_done_callback(...)`.
    /// Without these AppTask previously raised AttributeError on every
    /// sync route/dependency.
    callbacks: Vec<(Py<PyAny>, Py<PyAny>)>,
}

#[pymethods]
impl AppTask {
    /// Future done-callback: resume the coroutine (Task.__wakeup mirror).
    fn _wake(slf: Bound<'_, Self>, py: Python<'_>, fut: Bound<'_, PyAny>) -> PyResult<()> {
        let throw = fut.call_method0(intern!(py, "result")).err();
        let out = AppTask::step(&slf, py, throw)?;
        AppTask::after_step(&slf, py, out)
    }

    /// Bare-yield resume path (sleep(0)-style cooperative yields).
    fn _resume_bare(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let out = AppTask::step(&slf, py, None)?;
        AppTask::after_step(&slf, py, out)
    }

    // ---- minimal asyncio.Task surface (anyio compatibility) -----------

    #[pyo3(signature = (msg=None))]
    fn cancel(slf: Bound<'_, Self>, py: Python<'_>, msg: Option<Bound<'_, PyAny>>) -> PyResult<bool> {
        let waiting = {
            let mut this = slf.borrow_mut();
            if this.finished {
                return Ok(false);
            }
            match this.waiting_on.as_ref() {
                Some(f) => Some(f.clone_ref(py)),
                None => {
                    this.must_cancel = true;
                    None
                }
            }
        };
        if let Some(fut) = waiting {
            // Task.cancel semantics: cancel the awaited future; the
            // CancelledError arrives through _wake.
            let forwarded: bool = match msg {
                Some(m) => fut.call_method1(py, intern!(py, "cancel"), (m,))?,
                None => fut.call_method0(py, intern!(py, "cancel"))?,
            }
            .extract(py)
            .unwrap_or(false);
            if !forwarded {
                // The awaited future completed first, so cancelling it was a
                // no-op and no CancelledError will arrive through _wake —
                // the queued wake would otherwise resume the coroutine with
                // the successful result and silently swallow the cancel.
                // asyncio.Task latches the request instead; mirror that so
                // request timeouts and anyio cancel scopes still fire.
                slf.borrow_mut().must_cancel = true;
            }
        }
        Ok(true)
    }

    fn done(&self) -> bool {
        self.finished
    }

    fn cancelled(&self) -> bool {
        false
    }

    fn cancelling(&self) -> u32 {
        0
    }

    fn uncancel(&self) -> u32 {
        0
    }

    /// asyncio.Task private surface read directly (not called) by anyio's
    /// `CancelScope._deliver_cancellation` and
    /// `TaskInfo.has_pending_cancellation` — without these, any Starlette/
    /// FastAPI cancel scope raises AttributeError mid-cancellation.
    #[getter(_must_cancel)]
    fn must_cancel_attr(&self) -> bool {
        self.must_cancel
    }

    #[getter(_fut_waiter)]
    fn fut_waiter_attr(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.waiting_on.as_ref().map(|f| f.clone_ref(py))
    }

    fn get_loop(&self, py: Python<'_>) -> Py<PyAny> {
        self.pyloop.clone_ref(py)
    }

    /// Read directly (not called) by anyio's `WorkerThread.__init__`
    /// (`root_task._loop`) when resolving the loop to hand a
    /// thread-offloaded call back to.
    #[getter(_loop)]
    fn loop_attr(&self, py: Python<'_>) -> Py<PyAny> {
        self.pyloop.clone_ref(py)
    }

    fn get_coro(&self, py: Python<'_>) -> Py<PyAny> {
        self.coro.clone_ref(py)
    }

    fn get_name(&self) -> &'static str {
        "cadeloop.AppTask"
    }

    /// Future/Task protocol (mirrors asyncio.Future.add_done_callback):
    /// pending -> queued for firing when the task finishes; already-done
    /// -> scheduled via call_soon immediately, same as real asyncio.
    #[pyo3(signature = (fn_, *, context=None))]
    fn add_done_callback(
        slf: Bound<'_, Self>,
        py: Python<'_>,
        fn_: Py<PyAny>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let ctx = match context {
            Some(c) => c,
            None => crate::coreloop::copy_context(py)?,
        };
        let finished = slf.borrow().finished;
        if finished {
            AppTask::schedule_callback(&slf, py, fn_, ctx)?;
        } else {
            slf.borrow_mut().callbacks.push((fn_, ctx));
        }
        Ok(())
    }

    /// Mirrors asyncio.Future.remove_done_callback: drops every
    /// registered entry equal (by `==`, e.g. anyio compares bound
    /// methods) to `fn_`, returns the count removed.
    fn remove_done_callback(&mut self, py: Python<'_>, fn_: Py<PyAny>) -> PyResult<usize> {
        let target = fn_.bind(py);
        let before = self.callbacks.len();
        let mut err = None;
        self.callbacks.retain(|(cb, _)| match cb.bind(py).eq(target) {
            Ok(same) => !same,
            Err(e) => {
                err.get_or_insert(e);
                true
            }
        });
        if let Some(e) = err {
            return Err(e);
        }
        Ok(before - self.callbacks.len())
    }

    fn __repr__(&self) -> String {
        format!("<cadeloop.AppTask tid={} done={}>", self.tid, self.finished)
    }
}

/// The loop a future is attached to, mirroring `asyncio.futures._get_loop`:
/// `get_loop()` when it exists, else the `_loop` attribute. `None` when
/// neither is available -- an awaitable too exotic to judge, left alone.
fn future_loop<'py>(py: Python<'py>, fut: &Bound<'py, PyAny>) -> Option<Bound<'py, PyAny>> {
    if let Ok(get_loop) = fut.getattr(intern!(py, "get_loop")) {
        return get_loop.call0().ok();
    }
    fut.getattr(intern!(py, "_loop")).ok()
}

impl AppTask {
    pub(crate) fn spawn(
        py: Python<'_>,
        slf: &Bound<'_, CoreLoop>,
        tid: u64,
        coro: Py<PyAny>,
        pyloop: Py<PyAny>,
    ) -> PyResult<Py<AppTask>> {
        let context = crate::coreloop::copy_context(py)?;
        Py::new(
            py,
            AppTask {
                core: slf.clone().unbind(),
                tid,
                coro,
                pyloop,
                finished: false,
                must_cancel: false,
                waiting_on: None,
                context,
                callbacks: Vec::new(),
            },
        )
    }

    /// call_soon(callback, self, context=ctx) — the exact scheduling this
    /// codebase already uses for the bare-yield resume path (below), just
    /// with the task itself as the sole positional argument.
    fn schedule_callback(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        callback: Py<PyAny>,
        ctx: Py<PyAny>,
    ) -> PyResult<()> {
        let core_py = slf.borrow().core.clone_ref(py);
        let core_bound = core_py.bind(py);
        let core = core_bound.get();
        let self_arg = PyTuple::new(py, [slf.clone().into_any()])?;
        let handle =
            core.make_handle(py, callback.bind(py), &self_arg, Some(ctx.bind(py)), "add_done_callback")?;
        let token: Py<PyAny> = Py::new(py, handle)?.into_any();
        core.with_net(move |_, reactor| reactor.push_ready(token))?;
        Ok(())
    }

    /// Future.__schedule_callbacks mirror: fires once, when the task
    /// transitions to done (Finished/Failed). Failures here are reported
    /// unraisable rather than propagated — a broken done-callback must
    /// not corrupt the request whose completion triggered it, matching
    /// the _leave_task error handling right above in step().
    fn fire_done_callbacks(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let callbacks = std::mem::take(&mut slf.borrow_mut().callbacks);
        for (cb, ctx) in callbacks {
            if let Err(e) = AppTask::schedule_callback(slf, py, cb, ctx) {
                e.write_unraisable(py, None);
            }
        }
        Ok(())
    }

    /// Step the coroutine until it suspends or finishes. Runs OUTSIDE the
    /// state cell. Never pumps — completion handling is the caller's job
    /// (keeps pipelined bursts iterative).
    pub(crate) fn step(slf: &Bound<'_, Self>, py: Python<'_>, throw: Option<PyErr>) -> PyResult<StepOutcome> {
        let pyloop = {
            let mut this = slf.borrow_mut();
            this.waiting_on = None;
            this.pyloop.clone_ref(py)
        };
        // Register as asyncio.current_task() for the duration of the step
        // (anyio/Starlette task groups weakref the current task).
        task_hook(py, &ENTER_TASK, "_enter_task")?.call1(py, (&pyloop, slf))?;
        let out = Self::step_inner(slf, py, throw);
        if let Err(e) = task_hook(py, &LEAVE_TASK, "_leave_task").and_then(|f| f.call1(py, (&pyloop, slf))) {
            e.write_unraisable(py, None);
        }
        if matches!(out, Ok(StepOutcome::Finished) | Ok(StepOutcome::Failed)) {
            slf.borrow_mut().finished = true;
            AppTask::fire_done_callbacks(slf, py)?;
        }
        out
    }

    /// Enters the Context captured at spawn() around the whole step
    /// (mirrors run_handle's enter -> call -> always-exit discipline in
    /// handles.rs) so contextvars stay isolated per request, then
    /// delegates to step_body for the actual coroutine stepping.
    fn step_inner(slf: &Bound<'_, Self>, py: Python<'_>, throw: Option<PyErr>) -> PyResult<StepOutcome> {
        let ctx = slf.borrow().context.clone_ref(py);
        let ctx_ptr = ctx.as_ptr();
        if unsafe { ffi::PyContext_Enter(ctx_ptr) } != 0 {
            return Err(PyErr::fetch(py));
        }
        let result = AppTask::step_body(slf, py, throw);
        if unsafe { ffi::PyContext_Exit(ctx_ptr) } != 0 {
            // Context-stack corruption is a Rust-side invariant break —
            // surface it loudly rather than let it silently mis-scope a
            // later, unrelated step's contextvars.
            return Err(PyErr::fetch(py));
        }
        result
    }

    fn step_body(slf: &Bound<'_, Self>, py: Python<'_>, mut throw: Option<PyErr>) -> PyResult<StepOutcome> {
        let (coro, core_py, tid) = {
            let mut this = slf.borrow_mut();
            if this.must_cancel {
                this.must_cancel = false;
                throw = Some(cancelled_error(py));
            }
            (this.coro.clone_ref(py), this.core.clone_ref(py), this.tid)
        };
        let core = core_py.bind(py).get();
        loop {
            let step_result: Result<Option<Py<PyAny>>, PyErr> = if let Some(exc) = throw.take() {
                let res = coro.bind(py).call_method1(intern!(py, "throw"), (exc.into_value(py),));
                match res {
                    Ok(v) => Ok(Some(v.unbind())),
                    Err(e) if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => Ok(None),
                    Err(e) => Err(e),
                }
            } else {
                unsafe {
                    let mut result: *mut ffi::PyObject = std::ptr::null_mut();
                    let rc = ffi::PyIter_Send(coro.as_ptr(), py.None().as_ptr(), &mut result);
                    match rc {
                        ffi::PySendResult::PYGEN_RETURN => {
                            if !result.is_null() {
                                ffi::Py_DECREF(result);
                            }
                            Ok(None)
                        }
                        ffi::PySendResult::PYGEN_NEXT => Ok(Some(Bound::from_owned_ptr(py, result).unbind())),
                        ffi::PySendResult::PYGEN_ERROR => Err(PyErr::fetch(py)),
                    }
                }
            };
            match step_result {
                Ok(None) => return Ok(StepOutcome::Finished),
                Ok(Some(yielded)) => {
                    let y = yielded.bind(py);
                    if y.is_none() {
                        // bare `yield`: resume on the ready queue next tick.
                        let resume = slf.getattr(intern!(py, "_resume_bare"))?;
                        let h = core.make_handle(py, &resume, &PyTuple::empty(py), None, "AppTask")?;
                        let token: Py<PyAny> = Py::new(py, h)?.into_any();
                        core.with_net(move |_, reactor| reactor.push_ready(token))?;
                        return Ok(StepOutcome::Suspended);
                    }
                    // asyncio future protocol (Task.__step semantics).
                    match y.getattr(intern!(py, "_asyncio_future_blocking")) {
                        Ok(flag) if !flag.is_none() => {
                            // A Future belonging to another loop must not
                            // simply be subscribed to. If that loop is not
                            // running the request hangs for good; if it is
                            // running on another thread, `_wake` fires
                            // there and StateCell rejects the cross-thread
                            // access, leaving the request wedged behind a
                            // logged exception. asyncio.Task raises into
                            // the coroutine instead, which reaches the
                            // app-failure path and ends the request --
                            // match it (Task.__step_run_and_handle).
                            if let (Some(fut_loop), Ok(owner)) = (future_loop(py, y), core.require_owner()) {
                                if !fut_loop.is(owner.bind(py)) {
                                    throw = Some(PyRuntimeError::new_err(format!(
                                        "Task got Future {} attached to a different loop",
                                        y.repr()?
                                    )));
                                    continue;
                                }
                            }
                            y.setattr(intern!(py, "_asyncio_future_blocking"), false)?;
                            let wake = slf.getattr(intern!(py, "_wake"))?;
                            y.call_method1(intern!(py, "add_done_callback"), (wake,))?;
                            slf.borrow_mut().waiting_on = Some(yielded.clone_ref(py));
                            return Ok(StepOutcome::Suspended);
                        }
                        _ => {
                            throw =
                                Some(PyRuntimeError::new_err(format!("Task got bad yield: {}", y.repr()?)));
                        }
                    }
                }
                Err(e) => {
                    app_failure(py, core, tid, e)?;
                    return Ok(StepOutcome::Failed);
                }
            }
        }
    }

    /// Shared post-step handling for the resume paths (_wake /
    /// _resume_bare). The initial eager step in pump_requests handles its
    /// outcomes inline instead.
    fn after_step(slf: &Bound<'_, Self>, py: Python<'_>, out: StepOutcome) -> PyResult<()> {
        if matches!(out, StepOutcome::Suspended) {
            return Ok(());
        }
        let (core_py, tid) = {
            let this = slf.borrow();
            (this.core.clone_ref(py), this.tid)
        };
        let core_bound = core_py.bind(py);
        let core = core_bound.get();
        if matches!(out, StepOutcome::Finished) {
            on_coro_finished(py, core, tid)?;
        }
        drop_driver(py, core, tid)?;
        // Pump pipelined requests that queued while this one ran (R-085).
        pump_requests(py, core_bound, tid)
    }
}

/// Done-callback for the non-eager (stdlib asyncio.Task) path.
#[pyclass(frozen, module = "cadeloop._core")]
pub struct HttpTaskDone {
    core: Py<CoreLoop>,
    tid: u64,
}

#[pymethods]
impl HttpTaskDone {
    fn __call__(&self, py: Python<'_>, task: Bound<'_, PyAny>) -> PyResult<()> {
        let core_bound = self.core.bind(py);
        let core = core_bound.get();
        let cancelled: bool =
            task.call_method0(intern!(py, "cancelled")).and_then(|v| v.extract()).unwrap_or(false);
        if cancelled {
            app_failure(py, core, self.tid, PyRuntimeError::new_err("ASGI application task cancelled"))?;
        } else {
            let exc = task.call_method0(intern!(py, "exception"))?;
            if exc.is_none() {
                on_coro_finished(py, core, self.tid)?;
            } else {
                app_failure(py, core, self.tid, PyErr::from_value(exc))?;
            }
        }
        pump_requests(py, core_bound, self.tid)
    }
}
