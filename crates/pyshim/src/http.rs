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
use pyo3::sync::GILOnceCell;
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
    /// A request is being processed (its app coroutine has not returned).
    pub(crate) active: bool,
    pub(crate) keep_alive: bool,
    pub(crate) resp: RespPhase,
    pub(crate) resp_head: Vec<u8>,
    /// http.response.start carried a content-length header.
    pub(crate) resp_started_with_length: bool,
    /// Streaming without chunked framing (caller CL or close-delimited).
    pub(crate) raw_stream: bool,
    /// Active request is HEAD: body bytes are suppressed on the wire.
    pub(crate) active_head: bool,
    /// Active request's HTTP minor version (0 => chunked is unavailable).
    pub(crate) active_minor: u8,
    /// Body of the ACTIVE request, taken by the first `receive()`.
    pub(crate) req_body: Option<Vec<u8>>,
    pub(crate) disconnected: bool,
    /// Pending receive() waiter, resolved with http.disconnect.
    pub(crate) recv_waiter: Option<Py<PyAny>>,
    /// Cached per-connection ASGI callables (R-083).
    pub(crate) recv_obj: Option<Py<HttpReceive>>,
    pub(crate) send_obj: Option<Py<HttpSend>>,
    /// Suspended continuation (eager path) — kept alive here.
    pub(crate) driver: Option<Py<AppTask>>,
    pub(crate) eager: bool,
}

impl HttpConn {
    pub(crate) fn new(
        app: Py<PyAny>,
        pyloop: Py<PyAny>,
        state: Py<PyAny>,
        limits: Limits,
        eager: bool,
    ) -> Self {
        HttpConn {
            app,
            pyloop,
            state,
            parser: HttpParser::new(limits),
            pending: VecDeque::new(),
            active: false,
            keep_alive: true,
            resp: RespPhase::Idle,
            resp_head: Vec::new(),
            resp_started_with_length: false,
            raw_stream: false,
            active_head: false,
            active_minor: 1,
            req_body: None,
            disconnected: false,
            recv_waiter: None,
            recv_obj: None,
            send_obj: None,
            driver: None,
            eager,
        }
    }

    pub(crate) fn take_recv_waiter(&mut self) -> Option<Py<PyAny>> {
        self.recv_waiter.take()
    }
}

/// In-cell: feed received bytes into the parser. `Ok(true)` means phase 2
/// must run the request pump; parse errors are answered entirely in-cell
/// by the caller (R-086).
pub(crate) fn conn_feed(conn: &mut HttpConn, data: &[u8]) -> Result<bool, ParseError> {
    conn.parser.feed(data)?;
    while let Some(req) = conn.parser.next_request() {
        conn.pending.push_back(req);
    }
    Ok(!conn.pending.is_empty() && !conn.active)
}

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

static ASGI_INFO: GILOnceCell<Py<PyDict>> = GILOnceCell::new();
static EMPTY_BYTES: GILOnceCell<Py<PyBytes>> = GILOnceCell::new();
static COMPLETED: GILOnceCell<Py<CompletedAwaitable>> = GILOnceCell::new();

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

fn build_scope<'py>(
    py: Python<'py>,
    req: &Request,
    peer: Option<&Py<PyAny>>,
    local: Option<&Py<PyAny>>,
    state: &Py<PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    let scope = PyDict::new(py);
    scope.set_item(intern!(py, "type"), intern!(py, "http"))?;
    let asgi = ASGI_INFO.get_or_try_init(py, || -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        d.set_item(intern!(py, "version"), intern!(py, "3.0"))?;
        d.set_item(intern!(py, "spec_version"), intern!(py, "2.3"))?;
        Ok(d.unbind())
    })?;
    scope.set_item(intern!(py, "asgi"), asgi)?;
    scope.set_item(
        intern!(py, "http_version"),
        if req.http_minor == 1 { intern!(py, "1.1") } else { intern!(py, "1.0") },
    )?;
    scope.set_item(intern!(py, "method"), method_obj(py, req.method))?;
    scope.set_item(intern!(py, "scheme"), intern!(py, "http"))?;

    // Split path / query, decode path (R-081: percent-decoded, UTF-8 with
    // latin-1 fallback).
    let q = req.url.iter().position(|&b| b == b'?');
    let (raw_path, query) = match q {
        Some(i) => (&req.url[..i], &req.url[i + 1..]),
        None => (&req.url[..], &b""[..]),
    };
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
        headers.append(PyTuple::new(py, [PyBytes::new(py, name), PyBytes::new(py, value)])?)?;
    }
    scope.set_item(intern!(py, "headers"), headers)?;
    match peer {
        Some(p) => scope.set_item(intern!(py, "client"), p)?,
        None => scope.set_item(intern!(py, "client"), py.None())?,
    }
    match local {
        Some(p) => scope.set_item(intern!(py, "server"), p)?,
        None => scope.set_item(intern!(py, "server"), py.None())?,
    }
    // R-081: lifespan state, shallow-copied per request (ASGI spec).
    let state_copy = match state.bind(py).downcast::<PyDict>() {
        Ok(d) if !d.is_empty() => d.copy()?,
        _ => PyDict::new(py),
    };
    scope.set_item(intern!(py, "state"), state_copy)?;
    Ok(scope)
}

pub(crate) fn disconnect_message(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let d = PyDict::new(py);
    d.set_item(intern!(py, "type"), intern!(py, "http.disconnect"))?;
    Ok(d.into_any().unbind())
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
        }
        let r = core.with_net(|net, _| match net.http_conn_mut(self.tid) {
            None => R::Disconnect,
            Some(conn) => {
                if conn.disconnected {
                    R::Disconnect
                } else if let Some(b) = conn.req_body.take() {
                    R::Body(b)
                } else {
                    // Body already delivered: resolve on disconnect (or when
                    // this request finishes) — Starlette's disconnect
                    // listeners await here.
                    R::Wait(conn.pyloop.clone_ref(py))
                }
            }
        })?;
        match r {
            R::Disconnect => value_awaitable(py, disconnect_message(py)?),
            R::Body(b) => {
                let msg = PyDict::new(py);
                msg.set_item(intern!(py, "type"), intern!(py, "http.request"))?;
                let body_obj = if b.is_empty() {
                    EMPTY_BYTES
                        .get_or_init(py, || PyBytes::new(py, b"").unbind())
                        .bind(py)
                        .clone()
                        .into_any()
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
                let (stored, old) = core.with_net(move |net, _| {
                    match net.http_conn_mut(self.tid) {
                        Some(conn) => (true, conn.recv_waiter.replace(store)),
                        None => (false, Some(store)),
                    }
                })?;
                if let Some(old) = old {
                    core.with_net(|net, _| net.graveyard_py.push(old))?;
                    core.drain_graveyards(py)?;
                }
                if !stored {
                    let _ = fut.call_method1(intern!(py, "set_result"), (disconnect_message(py)?,));
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
    fn __call__(
        &self,
        py: Python<'_>,
        message: Bound<'_, PyDict>,
    ) -> PyResult<Py<CompletedAwaitable>> {
        let core = self.core.bind(py).get();
        process_send(py, core, self.tid, &message)?;
        Ok(completed(py).clone_ref(py))
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
/// R-086 protocol violations raise RuntimeError).
fn process_send(py: Python<'_>, core: &CoreLoop, tid: u64, message: &Bound<'_, PyDict>) -> PyResult<()> {
    let mtype: Bound<'_, PyAny> = message
        .get_item(intern!(py, "type"))?
        .ok_or_else(|| PyRuntimeError::new_err("ASGI message missing 'type'"))?;
    let mtype = mtype.downcast::<PyString>().map_err(PyErr::from)?;
    let kind = mtype.to_str()?;

    if kind == "http.response.start" {
        let status: u16 = message
            .get_item(intern!(py, "status"))?
            .ok_or_else(|| PyRuntimeError::new_err("http.response.start missing 'status'"))?
            .extract()?;
        // Serialize caller headers OUTSIDE the cell (arbitrary Python
        // objects), then commit in-cell.
        let mut head: Vec<u8> = Vec::with_capacity(256);
        head.extend_from_slice(b"HTTP/1.1 ");
        head.extend_from_slice(status.to_string().as_bytes());
        head.push(b' ');
        head.extend_from_slice(status_text(status).as_bytes());
        head.extend_from_slice(b"\r\n");
        let mut saw_length = false;
        let mut saw_close = false;
        if let Some(headers) = message.get_item(intern!(py, "headers"))? {
            for item in headers.try_iter()? {
                let pair = item?;
                let name: Vec<u8> = pair.get_item(0)?.extract()?;
                let value: Vec<u8> = pair.get_item(1)?.extract()?;
                if name.eq_ignore_ascii_case(b"content-length") {
                    saw_length = true;
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
            conn.resp_started_with_length = saw_length;
            if saw_close {
                conn.keep_alive = false;
            }
            conn.resp = RespPhase::Started;
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
                RespPhase::Started => {
                    // First body chunk decides the framing (R-084).
                    let mut out = std::mem::take(&mut conn.resp_head);
                    let saw_length = conn.resp_started_with_length;
                    let head_req = conn.active_head;
                    let minor = conn.active_minor;
                    if !more {
                        if !saw_length {
                            out.extend_from_slice(b"content-length: ");
                            out.extend_from_slice(body.len().to_string().as_bytes());
                            out.extend_from_slice(b"\r\n");
                        }
                        out.extend_from_slice(b"\r\n");
                        if !head_req {
                            out.extend_from_slice(&body);
                        }
                        conn.resp = RespPhase::Done;
                    } else if saw_length || head_req || minor == 0 {
                        // Raw streaming: caller-framed (their CL), a HEAD
                        // response (no body bytes at all), or HTTP/1.0
                        // (close-delimited — chunked needs 1.1).
                        out.extend_from_slice(b"\r\n");
                        if !head_req {
                            out.extend_from_slice(&body);
                        }
                        if minor == 0 && !saw_length {
                            conn.keep_alive = false;
                        }
                        conn.raw_stream = true;
                        conn.resp = RespPhase::Streaming;
                    } else {
                        out.extend_from_slice(b"transfer-encoding: chunked\r\n\r\n");
                        if !body.is_empty() {
                            push_chunk(&mut out, &body);
                        }
                        conn.raw_stream = false;
                        conn.resp = RespPhase::Streaming;
                    }
                    net::http_enqueue(py, net, backend, tid, out);
                    Ok(())
                }
                RespPhase::Streaming => {
                    let raw = conn.raw_stream;
                    let head_req = conn.active_head;
                    let mut out = Vec::with_capacity(body.len() + 16);
                    if head_req {
                        // body bytes suppressed
                    } else if raw {
                        out.extend_from_slice(&body);
                    } else if !body.is_empty() {
                        push_chunk(&mut out, &body);
                    }
                    if !more {
                        if !raw && !head_req {
                            out.extend_from_slice(b"0\r\n\r\n");
                        }
                        conn.resp = RespPhase::Done;
                    }
                    net::http_enqueue(py, net, backend, tid, out);
                    Ok(())
                }
            }
        })?
        .map_err(send_err)?;
        return Ok(());
    }

    Err(PyRuntimeError::new_err(format!("unsupported ASGI message type: {mtype}")))
}

fn push_chunk(out: &mut Vec<u8>, body: &[u8]) {
    out.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(b"\r\n");
}

// --------------------------------------------------------------------- //
// request pump + completion handling (R-056, R-085)                     //
// --------------------------------------------------------------------- //

/// Phase 2: start queued requests until the queue is empty or one
/// suspends. ITERATIVE — a pipelined burst of N sync requests runs in one
/// loop, not N nested stacks (AppTask::step never pumps).
pub(crate) fn pump_requests(py: Python<'_>, slf: &Bound<'_, CoreLoop>, tid: u64) -> PyResult<()> {
    let core = slf.get();
    loop {
        // Pop the next request only when no request is active.
        let next = core.with_net(|net, _| {
            let conn = net.http_conn_mut(tid)?;
            if conn.active {
                return None;
            }
            conn.pending.pop_front().map(|mut req| {
                conn.active = true;
                conn.keep_alive = req.keep_alive;
                conn.resp = RespPhase::Idle;
                conn.resp_head = Vec::new();
                conn.resp_started_with_length = false;
                conn.raw_stream = false;
                conn.active_head = req.method == "HEAD";
                conn.active_minor = req.http_minor;
                conn.req_body = Some(std::mem::take(&mut req.body));
                (
                    req,
                    conn.app.clone_ref(py),
                    conn.state.clone_ref(py),
                    conn.pyloop.clone_ref(py),
                    conn.eager,
                )
            })
        })?;
        let Some((req, app, state, pyloop, eager)) = next else { return Ok(()) };

        let (peer, local) = core.with_net(|net, _| net.peer_local(py, tid))?;
        let scope = build_scope(py, &req, peer.as_ref(), local.as_ref(), &state)?;
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
            match AppTask::step(task.bind(py), py, None)? {
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
            let task = pyloop.bind(py).call_method1(intern!(py, "create_task"), (coro,))?;
            let cb = Py::new(py, HttpTaskDone { core: slf.clone().unbind(), tid })?;
            task.call_method1(intern!(py, "add_done_callback"), (cb,))?;
            return Ok(());
        }
    }
}

/// The app coroutine returned: verify the response completed (R-086) and
/// finish the request cycle.
fn on_coro_finished(py: Python<'_>, core: &CoreLoop, tid: u64) -> PyResult<()> {
    let complete = core.with_net(|net, _| {
        net.http_conn_mut(tid).map(|c| c.resp == RespPhase::Done).unwrap_or(true)
    })?;
    if complete {
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
    let disconnect = err.is_instance_of::<PyConnectionResetError>(py)
        || err.is_instance_of::<PyBrokenPipeError>(py);
    if !disconnect {
        core.report_net_error(py, "Exception in ASGI application", err.into_value(py).into_any());
    }
    let phase =
        core.with_net(|net, _| net.http_conn_mut(tid).map(|c| c.resp))?;
    match phase {
        None => Ok(()), // connection already gone
        Some(RespPhase::Done) => {
            // Post-response failure (background task): the response is
            // intact — just finish the cycle.
            finish_request(py, core, tid)
        }
        Some(RespPhase::Idle) => {
            core.with_net(|net, reactor| {
                let backend = reactor.backend_mut();
                if let Some(c) = net.http_conn_mut(tid) {
                    c.resp = RespPhase::Done;
                    c.keep_alive = false;
                }
                let body =
                    error_response(ParseError { status: 500, reason: "internal server error" });
                net::http_enqueue(py, net, backend, tid, body);
                net::http_close_after_write(py, net, backend, tid);
            })?;
            core.drain_graveyards(py)
        }
        Some(_) => {
            // Mid-response: the framing cannot be repaired — hard close.
            core.with_net(|net, reactor| {
                net::teardown_with(py, net, reactor.backend_mut(), tid, None)
            })?;
            core.drain_graveyards(py)
        }
    }
}

/// Reset per-request state; close (`connection: close` / disconnect) or
/// continue keep-alive. The caller pumps pipelined requests (R-085).
pub(crate) fn finish_request(py: Python<'_>, core: &CoreLoop, tid: u64) -> PyResult<()> {
    let waiter = core.with_net(|net, reactor| {
        let backend = reactor.backend_mut();
        let (waiter, driver, close) = {
            let conn = net.http_conn_mut(tid)?;
            conn.active = false;
            conn.req_body = None;
            (
                conn.take_recv_waiter(),
                conn.driver.take(),
                !conn.keep_alive || conn.disconnected,
            )
        };
        if let Some(d) = driver {
            net.graveyard_py.push(d.into_any());
        }
        if close {
            net::http_close_after_write(py, net, backend, tid);
        }
        waiter
    })?;
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
static ENTER_TASK: GILOnceCell<Py<PyAny>> = GILOnceCell::new();
static LEAVE_TASK: GILOnceCell<Py<PyAny>> = GILOnceCell::new();
static CANCELLED_ERROR: GILOnceCell<Py<PyAny>> = GILOnceCell::new();

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
    cell: &'static GILOnceCell<Py<PyAny>>,
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
            match msg {
                Some(m) => fut.call_method1(py, intern!(py, "cancel"), (m,))?,
                None => fut.call_method0(py, intern!(py, "cancel"))?,
            };
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

    fn get_coro(&self, py: Python<'_>) -> Py<PyAny> {
        self.coro.clone_ref(py)
    }

    fn get_name(&self) -> &'static str {
        "cadeloop.AppTask"
    }

    fn __repr__(&self) -> String {
        format!("<cadeloop.AppTask tid={} done={}>", self.tid, self.finished)
    }
}

impl AppTask {
    pub(crate) fn spawn(
        py: Python<'_>,
        slf: &Bound<'_, CoreLoop>,
        tid: u64,
        coro: Py<PyAny>,
        pyloop: Py<PyAny>,
    ) -> PyResult<Py<AppTask>> {
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
            },
        )
    }

    /// Step the coroutine until it suspends or finishes. Runs OUTSIDE the
    /// state cell. Never pumps — completion handling is the caller's job
    /// (keeps pipelined bursts iterative).
    pub(crate) fn step(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        throw: Option<PyErr>,
    ) -> PyResult<StepOutcome> {
        let pyloop = {
            let mut this = slf.borrow_mut();
            this.waiting_on = None;
            this.pyloop.clone_ref(py)
        };
        // Register as asyncio.current_task() for the duration of the step
        // (anyio/Starlette task groups weakref the current task).
        task_hook(py, &ENTER_TASK, "_enter_task")?.call1(py, (&pyloop, slf))?;
        let out = Self::step_inner(slf, py, throw);
        if let Err(e) = task_hook(py, &LEAVE_TASK, "_leave_task")
            .and_then(|f| f.call1(py, (&pyloop, slf)))
        {
            e.write_unraisable(py, None);
        }
        if matches!(out, Ok(StepOutcome::Finished) | Ok(StepOutcome::Failed)) {
            slf.borrow_mut().finished = true;
        }
        out
    }

    fn step_inner(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        mut throw: Option<PyErr>,
    ) -> PyResult<StepOutcome> {
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
                        ffi::PySendResult::PYGEN_NEXT => Ok(Some(Py::from_owned_ptr(py, result))),
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
                            y.setattr(intern!(py, "_asyncio_future_blocking"), false)?;
                            let wake = slf.getattr(intern!(py, "_wake"))?;
                            y.call_method1(intern!(py, "add_done_callback"), (wake,))?;
                            slf.borrow_mut().waiting_on = Some(yielded.clone_ref(py));
                            return Ok(StepOutcome::Suspended);
                        }
                        _ => {
                            throw = Some(PyRuntimeError::new_err(format!(
                                "Task got bad yield: {}",
                                y.repr()?
                            )));
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
