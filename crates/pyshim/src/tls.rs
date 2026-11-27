//! Native TLS for stream transports (R-059): asyncio's `sslproto`, in Rust.
//!
//! `create_connection(ssl=...)`, `create_server(ssl=...)` and `start_tls()`
//! used to put the standard library's `asyncio.sslproto.SSLProtocol`
//! between the application's protocol and the native transport. That
//! class is correct and it is the reference for everything here, but it
//! is the slowest thing on a TLS connection: a dozen Python frames per
//! message around four C calls (profiled on a 1 KiB echo, the interpreter
//! was 37% of the server's CPU against 15% for uvloop's compiled port),
//! and a 256 KiB receive buffer, zero-filled, per connection.
//!
//! This module is the same state machine with the same observable
//! behavior -- the same states, the same callbacks in the same order, the
//! same exceptions and the same exception-handler reports -- driving the
//! same objects: the interpreter's own `SSLObject` over a pair of
//! `MemoryBIO`s, made by the caller's own `SSLContext`. Certificate
//! verification, protocol and cipher policy, SNI and session handling are
//! OpenSSL's through Python's `ssl` module, exactly as before; nothing
//! about TLS itself is reimplemented. What changes is who makes the calls
//! between them:
//!
//! * [`SslProtocol`] is the protocol the transport underneath sees. It is
//!   a plain `data_received` protocol: ciphertext goes from the
//!   transport's `bytes` straight into the incoming BIO, so there is no
//!   intermediate buffer to own.
//! * [`SslTransport`] is the transport the application sees
//!   (`_SSLProtocolTransport` in the standard library).
//!
//! Four deliberate differences from `sslproto`. The first three are not
//! visible to a protocol; the fourth is a bug of sslproto's that is not
//! reproduced:
//!
//! * `SSLObject.read()` is asked for one record's worth (16 KiB) at a
//!   time instead of 256 KiB: it returns at most one record either way,
//!   and `_ssl` allocates the size it is asked for before every call.
//! * After a read that returned data, the next read is skipped when
//!   there is nothing for it to find (the incoming BIO is empty and the
//!   SSL object holds no decrypted bytes), instead of being made for the
//!   `SSLWantReadError` that says so: raising and catching an exception
//!   per message was a measurable share of a small one.
//! * `close()` and `abort()` from a thread other than the loop's -- a
//!   garbage-collected `StreamWriter`, usually -- are handed to the loop's
//!   thread (ADR-36), rather than run the TLS shutdown, and the
//!   application's callbacks, on the thread that happened to collect it.
//! * A `BufferedProtocol` whose `get_buffer()` returns a `bytearray` gets
//!   its data. sslproto fills such a buffer through `buf[offset:]`, which
//!   for a bytearray is a copy, so on CPython 3.11-3.13 every TLS record
//!   after the first of a read is lost and the application is handed
//!   whatever its buffer held before (see `do_read_buffered`).
//!
//! Locking: the mutable state is atomics plus one `Mutex`, held only for
//! plain moves and reference clones and never across a call into Python
//! (which can re-enter any method here) or a drop of a Python reference
//! (which can run a finalizer).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard};

use pyo3::exceptions::{
    PyAssertionError, PyAttributeError, PyConnectionAbortedError, PyConnectionResetError, PyException,
    PyKeyboardInterrupt, PyNotImplementedError, PyOSError, PyRuntimeError, PySystemExit, PyTimeoutError,
    PyTypeError, PyValueError,
};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyByteArray, PyBytes, PyDict, PyList, PyMemoryView, PySlice, PyTuple};
use pyo3::{PyTraverseError, PyVisit};

use crate::coreloop::CoreLoop;
use crate::handles::CallArgs;

// SSLProtocolState
const UNWRAPPED: u8 = 0;
const DO_HANDSHAKE: u8 = 1;
const WRAPPED: u8 = 2;
const FLUSHING: u8 = 3;
const SHUTDOWN: u8 = 4;
const STATE_NAMES: [&str; 5] = ["UNWRAPPED", "DO_HANDSHAKE", "WRAPPED", "FLUSHING", "SHUTDOWN"];

// AppProtocolState
const APP_INIT: u8 = 0;
const APP_CON_MADE: u8 = 1;
const APP_EOF: u8 = 2;
const APP_CON_LOST: u8 = 3;

/// One TLS record carries at most 2^14 bytes of plaintext, and one
/// `SSLObject.read()` returns at most one record.
const READ_MAX: usize = 16 * 1024;
/// asyncio.constants, in KiB.
const HIGH_WATER_SSL_READ: i64 = 256;
const HIGH_WATER_SSL_WRITE: i64 = 512;
const LOG_THRESHOLD_FOR_CONNLOST_WRITES: usize = 5;
const SSL_HANDSHAKE_TIMEOUT: f64 = 60.0;
const SSL_SHUTDOWN_TIMEOUT: f64 = 30.0;

/// What this module needs from the standard library, looked up once.
struct Std {
    want_read: Py<PyAny>,
    syscall_error: Py<PyAny>,
    ssl_error: Py<PyAny>,
    certificate_error: Py<PyAny>,
    memory_bio: Py<PyAny>,
    create_default_context: Py<PyAny>,
    logger: Py<PyAny>,
    cancelled_error: Py<PyAny>,
    buffered_protocol: Py<PyAny>,
    sendfile_fallback: Py<PyAny>,
}

static STD: PyOnceLock<Std> = PyOnceLock::new();

fn std(py: Python<'_>) -> PyResult<&'static Std> {
    STD.get_or_try_init(py, || {
        let ssl = py.import("ssl")?;
        let asyncio = py.import("asyncio")?;
        Ok(Std {
            want_read: ssl.getattr("SSLWantReadError")?.unbind(),
            syscall_error: ssl.getattr("SSLSyscallError")?.unbind(),
            ssl_error: ssl.getattr("SSLError")?.unbind(),
            certificate_error: ssl.getattr("CertificateError")?.unbind(),
            memory_bio: ssl.getattr("MemoryBIO")?.unbind(),
            create_default_context: ssl.getattr("create_default_context")?.unbind(),
            logger: py.import("asyncio.log")?.getattr("logger")?.unbind(),
            cancelled_error: asyncio.getattr("CancelledError")?.unbind(),
            buffered_protocol: asyncio.getattr("BufferedProtocol")?.unbind(),
            sendfile_fallback: py
                .import("asyncio.constants")?
                .getattr("_SendfileMode")?
                .getattr("FALLBACK")?
                .unbind(),
        })
    })
}

/// sslproto's `SSLAgainErrors`: the SSL object needs more from the peer
/// before it can go on.
fn is_again(py: Python<'_>, e: &PyErr) -> PyResult<bool> {
    let s = std(py)?;
    Ok(e.is_instance(py, s.want_read.bind(py)) || e.is_instance(py, s.syscall_error.bind(py)))
}

/// `except Exception` -- what it catches, as opposed to a
/// KeyboardInterrupt or SystemExit on its way out.
fn is_exception(py: Python<'_>, e: &PyErr) -> bool {
    e.is_instance_of::<PyException>(py)
}

fn is_exit(py: Python<'_>, e: &PyErr) -> bool {
    e.is_instance_of::<PyKeyboardInterrupt>(py) || e.is_instance_of::<PySystemExit>(py)
}

/// What Python raises for `None.<name>`: sslproto drops its references
/// when the connection is lost, and what is called on it afterwards
/// fails this way (and is then handled as any other failure there).
fn none_has_no(name: &str) -> PyErr {
    PyAttributeError::new_err(format!("'NoneType' object has no attribute '{name}'"))
}

/// The application's protocol, with the methods the data path calls
/// looked up once (`_set_app_protocol`).
struct App {
    protocol: Py<PyAny>,
    /// `data_received`, or `get_buffer` and `buffer_updated` for a
    /// BufferedProtocol.
    data_received: Option<Py<PyAny>>,
    buffered: Option<(Py<PyAny>, Py<PyAny>)>,
}

impl App {
    fn new(protocol: &Bound<'_, PyAny>) -> PyResult<App> {
        let py = protocol.py();
        let is_buffer = protocol.hasattr(intern!(py, "get_buffer"))?
            && protocol.is_instance(std(py)?.buffered_protocol.bind(py))?;
        if is_buffer {
            let get_buffer = protocol.getattr(intern!(py, "get_buffer"))?.unbind();
            let buffer_updated = protocol.getattr(intern!(py, "buffer_updated"))?.unbind();
            Ok(App {
                protocol: protocol.clone().unbind(),
                data_received: None,
                buffered: Some((get_buffer, buffer_updated)),
            })
        } else {
            // Looked up lazily by sslproto (at the call); a protocol
            // without it fails there, on its first data. Same here.
            let data_received = protocol.getattr(intern!(py, "data_received")).ok().map(Bound::unbind);
            Ok(App { protocol: protocol.clone().unbind(), data_received, buffered: None })
        }
    }
}

#[derive(Default)]
struct Slots {
    app: Option<App>,
    app_transport: Option<Py<SslTransport>>,
    /// The transport underneath, and its `write`.
    transport: Option<Py<PyAny>>,
    transport_write: Option<Py<PyAny>>,
    waiter: Option<Py<PyAny>>,
    backlog: VecDeque<Py<PyAny>>,
    handshake_handle: Option<Py<PyAny>>,
    shutdown_handle: Option<Py<PyAny>>,
    handshake_start: Option<Py<PyAny>>,
    /// The four flow-control limits as given (what the getters return).
    outgoing_limits: Option<(Py<PyAny>, Py<PyAny>)>, // (high, low)
    incoming_limits: Option<(Py<PyAny>, Py<PyAny>)>,
}

/// A flow-control limit for comparing with byte counts: its numeric
/// value, as the bits of an f64 in an atomic.
struct Limit(AtomicU64);

impl Limit {
    fn new() -> Limit {
        Limit(AtomicU64::new(0f64.to_bits()))
    }
    fn set(&self, v: f64) {
        self.0.store(v.to_bits(), Relaxed);
    }
    fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Relaxed))
    }
}

/// asyncio's `sslproto.SSLProtocol`: the protocol under a TLS connection.
#[pyclass(frozen, weakref, module = "cadeloop._core", name = "SSLProtocol")]
pub struct SslProtocol {
    loop_: Py<PyAny>,
    /// The loop's core, when the loop is a cadeloop one: for telling
    /// which thread a call is on.
    core: Option<Py<CoreLoop>>,
    sslcontext: Py<PyAny>,
    server_side: bool,
    server_hostname: Option<Py<PyAny>>,
    handshake_timeout: Py<PyAny>,
    shutdown_timeout: Py<PyAny>,
    sslobj: Py<PyAny>,
    incoming: Py<PyAny>,
    outgoing: Py<PyAny>,
    // Bound methods of the three, looked up once.
    ssl_read: Py<PyAny>,
    ssl_write: Py<PyAny>,
    ssl_pending: Py<PyAny>,
    incoming_write: Py<PyAny>,
    outgoing_read: Py<PyAny>,
    extra: Py<PyDict>,
    state: AtomicU8,
    app_state: AtomicU8,
    conn_lost: AtomicUsize,
    eof_received: AtomicBool,
    ssl_writing_paused: AtomicBool,
    app_reading_paused: AtomicBool,
    ssl_reading_paused: AtomicBool,
    app_writing_paused: AtomicBool,
    app_transport_created: AtomicBool,
    write_buffer_size: AtomicUsize,
    outgoing_high: Limit,
    outgoing_low: Limit,
    incoming_high: Limit,
    incoming_low: Limit,
    slots: Mutex<Slots>,
}

type Proto<'a, 'py> = &'a Bound<'py, SslProtocol>;

/// asyncio's `add_flowcontrol_defaults`, in Python arithmetic so that any
/// real number works as it does there.
fn flowcontrol_defaults<'py>(
    py: Python<'py>,
    high: Option<Bound<'py, PyAny>>,
    low: Option<Bound<'py, PyAny>>,
    kb: i64,
) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
    let high = high.filter(|h| !h.is_none());
    let low = low.filter(|l| !l.is_none());
    let hi = match (&high, &low) {
        (Some(h), _) => h.clone(),
        (None, Some(l)) => l.mul(4)?,
        (None, None) => (kb * 1024).into_pyobject(py)?.into_any(),
    };
    let lo = match low {
        Some(l) => l,
        None => hi.floor_div(4)?,
    };
    if !(hi.ge(&lo)? && lo.ge(0)?) {
        return Err(PyValueError::new_err(format!(
            "high ({}) must be >= low ({}) must be >= 0",
            hi.repr()?,
            lo.repr()?
        )));
    }
    Ok((hi, lo))
}

/// `len(obj)`, as sslproto counts what it buffers.
fn py_len(obj: &Bound<'_, PyAny>) -> PyResult<usize> {
    obj.len()
}

impl SslProtocol {
    fn lock(&self) -> MutexGuard<'_, Slots> {
        self.slots.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn state(&self) -> u8 {
        self.state.load(Relaxed)
    }

    /// sslproto's `_set_state`: only the transitions of a TLS session, or
    /// back to UNWRAPPED from anywhere.
    fn set_state(&self, new: u8) -> PyResult<()> {
        let old = self.state();
        let allowed = new == UNWRAPPED
            || matches!(
                (old, new),
                (UNWRAPPED, DO_HANDSHAKE)
                    | (DO_HANDSHAKE, WRAPPED)
                    | (WRAPPED, FLUSHING)
                    | (FLUSHING, SHUTDOWN)
            );
        if !allowed {
            return Err(PyRuntimeError::new_err(format!(
                "cannot switch state from SSLProtocolState.{} to SSLProtocolState.{}",
                STATE_NAMES[old as usize], STATE_NAMES[new as usize]
            )));
        }
        self.state.store(new, Relaxed);
        Ok(())
    }

    fn transport(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.lock().transport.as_ref().map(|t| t.clone_ref(py))
    }

    fn app_protocol(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.lock().app.as_ref().map(|a| a.protocol.clone_ref(py))
    }

    fn app_transport(&self, py: Python<'_>) -> Option<Py<SslTransport>> {
        self.lock().app_transport.as_ref().map(|t| t.clone_ref(py))
    }

    fn debug(&self, py: Python<'_>) -> PyResult<bool> {
        self.loop_.bind(py).call_method0(intern!(py, "get_debug"))?.is_truthy()
    }

    fn cancel_handle(py: Python<'_>, handle: Option<Py<PyAny>>) -> PyResult<()> {
        if let Some(handle) = handle {
            handle.bind(py).call_method0(intern!(py, "cancel"))?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The state machine. One function per sslproto method, same names.
// ---------------------------------------------------------------------------

fn wakeup_waiter(slf: Proto<'_, '_>, exc: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    let py = slf.py();
    let Some(waiter) = slf.get().lock().waiter.take() else { return Ok(()) };
    let waiter = waiter.bind(py);
    if !waiter.call_method0(intern!(py, "cancelled"))?.is_truthy()? {
        match exc {
            Some(exc) => waiter.call_method1(intern!(py, "set_exception"), (exc,))?,
            None => waiter.call_method1(intern!(py, "set_result"), (py.None(),))?,
        };
    }
    Ok(())
}

fn get_app_transport(slf: Proto<'_, '_>) -> PyResult<Py<SslTransport>> {
    let py = slf.py();
    let this = slf.get();
    if let Some(t) = this.app_transport(py) {
        return Ok(t);
    }
    if this.app_transport_created.load(Relaxed) {
        return Err(PyRuntimeError::new_err("Creating _SSLProtocolTransport twice"));
    }
    let transport = Py::new(
        py,
        SslTransport {
            loop_: this.loop_.clone_ref(py),
            ssl_protocol: Mutex::new(Some(slf.clone().unbind())),
            closed: AtomicBool::new(false),
            close_handed: AtomicBool::new(false),
        },
    )?;
    this.lock().app_transport = Some(transport.clone_ref(py));
    this.app_transport_created.store(true, Relaxed);
    Ok(transport)
}

fn start_handshake(slf: Proto<'_, '_>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let start = if this.debug(py)? {
        std(py)?.logger.bind(py).call_method1(intern!(py, "debug"), ("%r starts SSL handshake", slf))?;
        Some(this.loop_.bind(py).call_method0(intern!(py, "time"))?.unbind())
    } else {
        None
    };
    let old = std::mem::replace(&mut this.lock().handshake_start, start);
    drop(old);
    this.set_state(DO_HANDSHAKE)?;
    let handle = this.loop_.bind(py).call_method1(
        intern!(py, "call_later"),
        (this.handshake_timeout.bind(py), slf.getattr(intern!(py, "_check_handshake_timeout"))?),
    )?;
    let old = this.lock().handshake_handle.replace(handle.unbind());
    drop(old);
    do_handshake(slf)
}

fn do_handshake(slf: Proto<'_, '_>) -> PyResult<()> {
    let py = slf.py();
    match slf.get().sslobj.bind(py).call_method0(intern!(py, "do_handshake")) {
        Ok(_) => on_handshake_complete(slf, None),
        Err(e) if is_again(py, &e)? => process_outgoing(slf),
        Err(e) if e.is_instance(py, std(py)?.ssl_error.bind(py)) => on_handshake_complete(slf, Some(e)),
        Err(e) => Err(e),
    }
}

fn on_handshake_complete(slf: Proto<'_, '_>, handshake_exc: Option<PyErr>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let handle = this.lock().handshake_handle.take();
    SslProtocol::cancel_handle(py, handle)?;
    let sslobj = this.sslobj.bind(py);
    let peercert = (|| {
        match handshake_exc {
            None => this.set_state(WRAPPED)?,
            Some(exc) => return Err(exc),
        }
        sslobj.call_method0(intern!(py, "getpeercert"))
    })();
    let peercert = match peercert {
        Ok(peercert) => peercert,
        Err(exc) if is_exception(py, &exc) => {
            this.set_state(UNWRAPPED)?;
            let msg = if exc.is_instance(py, std(py)?.certificate_error.bind(py)) {
                "SSL handshake failed on verifying the certificate"
            } else {
                "SSL handshake failed"
            };
            let value = exc.value(py).clone();
            fatal_error(slf, exc, msg)?;
            return wakeup_waiter(slf, Some(value.as_any()));
        }
        Err(exc) => return Err(exc),
    };
    if this.debug(py)? {
        let start = this.lock().handshake_start.as_ref().map(|s| s.clone_ref(py));
        if let Some(start) = start {
            let dt = this.loop_.bind(py).call_method0(intern!(py, "time"))?.sub(start.bind(py))?;
            std(py)?
                .logger
                .bind(py)
                .call_method1(intern!(py, "debug"), ("%r: SSL handshake took %.1f ms", slf, dt.mul(1e3)?))?;
        }
    }
    let extra = this.extra.bind(py);
    extra.set_item(intern!(py, "peercert"), peercert)?;
    extra.set_item(intern!(py, "cipher"), sslobj.call_method0(intern!(py, "cipher"))?)?;
    extra.set_item(intern!(py, "compression"), sslobj.call_method0(intern!(py, "compression"))?)?;
    extra.set_item(intern!(py, "ssl_object"), sslobj)?;
    if this.app_state.load(Relaxed) == APP_INIT {
        this.app_state.store(APP_CON_MADE, Relaxed);
        let app = this.app_protocol(py).ok_or_else(|| none_has_no("connection_made"))?;
        app.bind(py).call_method1(intern!(py, "connection_made"), (get_app_transport(slf)?,))?;
    }
    wakeup_waiter(slf, None)?;
    do_read(slf)
}

fn start_shutdown(slf: Proto<'_, '_>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    if matches!(this.state(), FLUSHING | SHUTDOWN | UNWRAPPED) {
        return Ok(());
    }
    if let Some(t) = this.app_transport(py) {
        t.get().closed.store(true, Relaxed);
    }
    if this.state() == DO_HANDSHAKE {
        abort(slf, None)
    } else {
        this.set_state(FLUSHING)?;
        let handle = this.loop_.bind(py).call_method1(
            intern!(py, "call_later"),
            (this.shutdown_timeout.bind(py), slf.getattr(intern!(py, "_check_shutdown_timeout"))?),
        )?;
        let old = this.lock().shutdown_handle.replace(handle.unbind());
        drop(old);
        do_flush(slf)
    }
}

fn do_flush(slf: Proto<'_, '_>) -> PyResult<()> {
    do_read(slf)?;
    slf.get().set_state(SHUTDOWN)?;
    do_shutdown(slf)
}

fn do_shutdown(slf: Proto<'_, '_>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let unwrapped = if this.eof_received.load(Relaxed) {
        Ok(())
    } else {
        this.sslobj.bind(py).call_method0(intern!(py, "unwrap")).map(drop)
    };
    match unwrapped {
        Ok(()) => {
            process_outgoing(slf)?;
            call_eof_received(slf)?;
            on_shutdown_complete(slf, None)
        }
        Err(e) if is_again(py, &e)? => process_outgoing(slf),
        Err(e) if e.is_instance(py, std(py)?.ssl_error.bind(py)) => on_shutdown_complete(slf, Some(e)),
        Err(e) => Err(e),
    }
}

fn on_shutdown_complete(slf: Proto<'_, '_>, shutdown_exc: Option<PyErr>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let handle = this.lock().shutdown_handle.take();
    SslProtocol::cancel_handle(py, handle)?;
    match shutdown_exc {
        Some(exc) => fatal_error(slf, exc, "Fatal error on transport"),
        None => {
            let transport = this.transport(py).ok_or_else(|| none_has_no("close"))?;
            let close = transport.bind(py).getattr(intern!(py, "close"))?;
            this.loop_.bind(py).call_method1(intern!(py, "call_soon"), (close,))?;
            Ok(())
        }
    }
}

fn abort(slf: Proto<'_, '_>, exc: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    this.set_state(UNWRAPPED)?;
    if let Some(transport) = this.transport(py) {
        transport.bind(py).call_method1(intern!(py, "_force_close"), (exc,))?;
    }
    Ok(())
}

fn write_appdata(slf: Proto<'_, '_>, list_of_data: &Bound<'_, PyAny>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    if matches!(this.state(), FLUSHING | SHUTDOWN | UNWRAPPED) {
        if this.conn_lost.load(Relaxed) >= LOG_THRESHOLD_FOR_CONNLOST_WRITES {
            std(py)?.logger.bind(py).call_method1(intern!(py, "warning"), ("SSL connection is closed",))?;
        }
        this.conn_lost.fetch_add(1, Relaxed);
        return Ok(());
    }
    for data in list_of_data.try_iter()? {
        let data = data?;
        let len = py_len(&data)?;
        this.lock().backlog.push_back(data.unbind());
        this.write_buffer_size.fetch_add(len, Relaxed);
    }
    if this.state() == WRAPPED {
        if let Err(e) = do_write(slf) {
            if !is_exception(py, &e) {
                return Err(e);
            }
            return fatal_error(slf, e, "Fatal error on SSL protocol");
        }
    }
    Ok(())
}

fn do_write(slf: Proto<'_, '_>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let write = this.ssl_write.bind(py);
    loop {
        let Some(data) = this.lock().backlog.front().map(|d| d.clone_ref(py)) else { break };
        let data = data.bind(py);
        match write.call1((data,)) {
            Ok(count) => {
                let count: usize = count.extract()?;
                let len = py_len(data)?;
                if count < len {
                    let rest = data.get_item(PySlice::new(py, count as isize, isize::MAX, 1))?;
                    let old =
                        this.lock().backlog.front_mut().map(|front| std::mem::replace(front, rest.unbind()));
                    drop(old);
                    this.write_buffer_size
                        .fetch_sub(count.min(this.write_buffer_size.load(Relaxed)), Relaxed);
                } else {
                    let old = this.lock().backlog.pop_front();
                    drop(old);
                    this.write_buffer_size.fetch_sub(len.min(this.write_buffer_size.load(Relaxed)), Relaxed);
                }
            }
            Err(e) if is_again(py, &e)? => break,
            Err(e) => return Err(e),
        }
    }
    process_outgoing(slf)
}

fn process_outgoing(slf: Proto<'_, '_>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let mut drained = false;
    if !this.ssl_writing_paused.load(Relaxed) {
        let data = this.outgoing_read.bind(py).call0()?;
        if py_len(&data)? > 0 {
            let write = this.lock().transport_write.as_ref().map(|w| w.clone_ref(py));
            let write = write.ok_or_else(|| none_has_no("write"))?;
            write.bind(py).call1((data,))?;
        }
        drained = true;
    }
    control_app_writing(slf, drained)
}

fn do_read(slf: Proto<'_, '_>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    if !matches!(this.state(), WRAPPED | FLUSHING) {
        return Ok(());
    }
    let run = || -> PyResult<()> {
        if !this.app_reading_paused.load(Relaxed) {
            let buffered = this.lock().app.as_ref().is_some_and(|a| a.buffered.is_some());
            if buffered {
                do_read_buffered(slf)?;
            } else {
                do_read_copied(slf)?;
            }
            if this.lock().backlog.is_empty() {
                process_outgoing(slf)?;
            } else {
                do_write(slf)?;
            }
        }
        control_ssl_reading(slf)
    };
    match run() {
        Err(e) if is_exception(py, &e) => fatal_error(slf, e, "Fatal error on SSL protocol"),
        other => other,
    }
}

/// Is there anything a further `SSLObject.read()` could return right now?
/// False spares the call that would only raise SSLWantReadError.
fn more_to_read(slf: Proto<'_, '_>) -> PyResult<bool> {
    let py = slf.py();
    let this = slf.get();
    if this.incoming.bind(py).getattr(intern!(py, "pending"))?.extract::<usize>()? > 0 {
        return Ok(true);
    }
    Ok(this.ssl_pending.bind(py).call0()?.extract::<usize>()? > 0)
}

fn do_read_buffered(slf: Proto<'_, '_>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let (get_buffer, buffer_updated) = {
        let slots = this.lock();
        let (g, u) =
            slots.app.as_ref().and_then(|a| a.buffered.as_ref()).ok_or_else(|| none_has_no("get_buffer"))?;
        (g.clone_ref(py), u.clone_ref(py))
    };
    let mut offset = 0usize;
    let mut count = 1usize;
    let buf = get_buffer.bind(py).call1((get_read_buffer_size(slf)?,))?;
    let wants = py_len(&buf)?;
    // Always through a memoryview. sslproto slices what get_buffer()
    // returned (`buf[offset:]`), and slicing a bytearray copies it: every
    // record after the first is then read into a temporary and thrown
    // away, while the application is told its buffer holds it. A
    // BufferedProtocol that hands out a bytearray -- which the protocol
    // allows -- gets stale bytes in place of its data over TLS on CPython
    // 3.11-3.13. A view's slice is the buffer itself.
    let buf = PyMemoryView::from(&buf)?;
    let read = this.ssl_read.bind(py);
    let mut step = || -> PyResult<()> {
        count = read.call1((wants, &buf))?.extract()?;
        if count > 0 {
            offset = count;
            loop {
                if offset >= wants {
                    // The buffer is full and there may be more: come back
                    // for it once the application has had this much.
                    this.loop_
                        .bind(py)
                        .call_method1(intern!(py, "call_soon"), (slf.getattr(intern!(py, "_do_read"))?,))?;
                    break;
                }
                let rest = buf.get_item(PySlice::new(py, offset as isize, isize::MAX, 1))?;
                count = read.call1((wants - offset, rest))?.extract()?;
                if count > 0 {
                    offset += count;
                } else {
                    break;
                }
            }
        }
        Ok(())
    };
    match step() {
        Ok(()) => {}
        Err(e) if is_again(py, &e)? => {}
        Err(e) => return Err(e),
    }
    if offset > 0 {
        buffer_updated.bind(py).call1((offset,))?;
    }
    if count == 0 {
        call_eof_received(slf)?;
        start_shutdown(slf)?;
    }
    Ok(())
}

fn do_read_copied(slf: Proto<'_, '_>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let read = this.ssl_read.bind(py);
    let mut first: Option<Bound<'_, PyAny>> = None;
    let mut rest: Option<Bound<'_, PyList>> = None;
    let mut eof = false;
    loop {
        match read.call1((READ_MAX,)) {
            Ok(chunk) => {
                if py_len(&chunk)? == 0 {
                    eof = true;
                    break;
                }
                match (&first, &rest) {
                    (None, _) => first = Some(chunk),
                    (Some(f), None) => rest = Some(PyList::new(py, [f.clone(), chunk])?),
                    (Some(_), Some(list)) => list.append(chunk)?,
                }
                if !more_to_read(slf)? {
                    break;
                }
            }
            Err(e) if is_again(py, &e)? => break,
            Err(e) => return Err(e),
        }
    }
    if let Some(first) = first {
        let data_received =
            this.lock().app.as_ref().and_then(|a| a.data_received.as_ref().map(|d| d.clone_ref(py)));
        let data_received = match data_received {
            Some(d) => d,
            None => {
                // No cached method: the protocol is gone, or never had
                // one. Ask, for the error Python would give.
                let app = this.app_protocol(py).ok_or_else(|| none_has_no("data_received"))?;
                app.bind(py).getattr(intern!(py, "data_received"))?.unbind()
            }
        };
        match rest {
            None => data_received.bind(py).call1((first,))?,
            Some(list) => data_received
                .bind(py)
                .call1((PyBytes::new(py, b"").call_method1(intern!(py, "join"), (list,))?,))?,
        };
    }
    if eof {
        call_eof_received(slf)?;
        start_shutdown(slf)?;
    }
    Ok(())
}

fn call_eof_received(slf: Proto<'_, '_>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let run = || -> PyResult<()> {
        if this.app_state.load(Relaxed) == APP_CON_MADE {
            this.app_state.store(APP_EOF, Relaxed);
            let app = this.app_protocol(py).ok_or_else(|| none_has_no("eof_received"))?;
            let keep_open = app.bind(py).call_method0(intern!(py, "eof_received"))?;
            if keep_open.is_truthy()? {
                std(py)?.logger.bind(py).call_method1(
                    intern!(py, "warning"),
                    ("returning true from eof_received() has no effect when using ssl",),
                )?;
            }
        }
        Ok(())
    };
    match run() {
        Ok(()) => Ok(()),
        Err(e) if is_exit(py, &e) => Err(e),
        Err(e) => fatal_error(slf, e, "Error calling eof_received()"),
    }
}

/// `known_empty`: the outgoing BIO was just read out, so its `pending`
/// need not be asked for.
fn control_app_writing(slf: Proto<'_, '_>, known_empty: bool) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let buffered = this.write_buffer_size.load(Relaxed);
    let paused = this.app_writing_paused.load(Relaxed);
    // The common case, decided without touching the BIO: nothing queued
    // and nobody paused.
    if known_empty && buffered == 0 && !paused && this.outgoing_high.get() > 0.0 {
        return Ok(());
    }
    let pending = if known_empty {
        0
    } else {
        this.outgoing.bind(py).getattr(intern!(py, "pending"))?.extract::<usize>()?
    };
    let size = (pending + buffered) as f64;
    let method = if size >= this.outgoing_high.get() && !paused {
        this.app_writing_paused.store(true, Relaxed);
        "pause_writing"
    } else if size <= this.outgoing_low.get() && paused {
        this.app_writing_paused.store(false, Relaxed);
        "resume_writing"
    } else {
        return Ok(());
    };
    let called = match this.app_protocol(py) {
        Some(app) => app.bind(py).call_method0(method).map(drop),
        None => Err(none_has_no(method)),
    };
    match called {
        Ok(()) => Ok(()),
        Err(e) if is_exit(py, &e) => Err(e),
        Err(e) => {
            let context = PyDict::new(py);
            context.set_item("message", format!("protocol.{method}() failed"))?;
            context.set_item("exception", e.into_value(py))?;
            context.set_item("transport", this.app_transport(py))?;
            context.set_item("protocol", slf)?;
            this.loop_.bind(py).call_method1(intern!(py, "call_exception_handler"), (context,))?;
            Ok(())
        }
    }
}

fn get_write_buffer_size(slf: Proto<'_, '_>) -> PyResult<usize> {
    let py = slf.py();
    let this = slf.get();
    let pending: usize = this.outgoing.bind(py).getattr(intern!(py, "pending"))?.extract()?;
    Ok(pending + this.write_buffer_size.load(Relaxed))
}

fn get_read_buffer_size(slf: Proto<'_, '_>) -> PyResult<usize> {
    let py = slf.py();
    slf.get().incoming.bind(py).getattr(intern!(py, "pending"))?.extract()
}

fn control_ssl_reading(slf: Proto<'_, '_>) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let size = get_read_buffer_size(slf)? as f64;
    let paused = this.ssl_reading_paused.load(Relaxed);
    let method = if size >= this.incoming_high.get() && !paused {
        this.ssl_reading_paused.store(true, Relaxed);
        intern!(py, "pause_reading")
    } else if size <= this.incoming_low.get() && paused {
        this.ssl_reading_paused.store(false, Relaxed);
        intern!(py, "resume_reading")
    } else {
        return Ok(());
    };
    let transport = this.transport(py).ok_or_else(|| none_has_no("pause_reading"))?;
    transport.bind(py).call_method0(method)?;
    Ok(())
}

fn set_write_buffer_limits(
    slf: Proto<'_, '_>,
    high: Option<Bound<'_, PyAny>>,
    low: Option<Bound<'_, PyAny>>,
) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let (high, low) = flowcontrol_defaults(py, high, low, HIGH_WATER_SSL_WRITE)?;
    this.outgoing_high.set(high.extract()?);
    this.outgoing_low.set(low.extract()?);
    let old = this.lock().outgoing_limits.replace((high.unbind(), low.unbind()));
    drop(old);
    Ok(())
}

fn set_read_buffer_limits(
    slf: Proto<'_, '_>,
    high: Option<Bound<'_, PyAny>>,
    low: Option<Bound<'_, PyAny>>,
) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let (high, low) = flowcontrol_defaults(py, high, low, HIGH_WATER_SSL_READ)?;
    this.incoming_high.set(high.extract()?);
    this.incoming_low.set(low.extract()?);
    let old = this.lock().incoming_limits.replace((high.unbind(), low.unbind()));
    drop(old);
    Ok(())
}

/// The body of sslproto's `_resume_reading.resume`.
fn resume(slf: Proto<'_, '_>) -> PyResult<()> {
    match slf.get().state() {
        WRAPPED => do_read(slf),
        FLUSHING => do_flush(slf),
        SHUTDOWN => do_shutdown(slf),
        _ => Ok(()),
    }
}

fn fatal_error(slf: Proto<'_, '_>, exc: PyErr, message: &str) -> PyResult<()> {
    let py = slf.py();
    let this = slf.get();
    let transport = this.transport(py);
    let value = exc.value(py).clone();
    if let Some(transport) = &transport {
        transport.bind(py).call_method1(intern!(py, "_force_close"), (&value,))?;
    }
    if exc.is_instance_of::<PyOSError>(py) {
        if this.debug(py)? {
            let kwargs = PyDict::new(py);
            kwargs.set_item("exc_info", (exc.get_type(py), &value, exc.traceback(py)))?;
            std(py)?.logger.bind(py).call_method(
                intern!(py, "debug"),
                ("%r: %s", slf, message),
                Some(&kwargs),
            )?;
        }
    } else if !exc.is_instance(py, std(py)?.cancelled_error.bind(py)) {
        let context = PyDict::new(py);
        context.set_item("message", message)?;
        context.set_item("exception", &value)?;
        context.set_item("transport", transport)?;
        context.set_item("protocol", slf)?;
        this.loop_.bind(py).call_method1(intern!(py, "call_exception_handler"), (context,))?;
    }
    Ok(())
}

#[pymethods]
impl SslProtocol {
    #[new]
    #[pyo3(signature = (loop_, app_protocol, sslcontext, waiter, server_side=false, server_hostname=None,
                        call_connection_made=true, ssl_handshake_timeout=None, ssl_shutdown_timeout=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        loop_: Bound<'_, PyAny>,
        app_protocol: Bound<'_, PyAny>,
        sslcontext: Bound<'_, PyAny>,
        waiter: Bound<'_, PyAny>,
        server_side: bool,
        server_hostname: Option<Bound<'_, PyAny>>,
        call_connection_made: bool,
        ssl_handshake_timeout: Option<Bound<'_, PyAny>>,
        ssl_shutdown_timeout: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let s = std(py)?;
        let timeout = |given: Option<Bound<'_, PyAny>>, default: f64, name: &str| -> PyResult<Py<PyAny>> {
            match given.filter(|t| !t.is_none()) {
                None => Ok(default.into_pyobject(py)?.into_any().unbind()),
                Some(t) => {
                    if t.le(0)? {
                        return Err(PyValueError::new_err(format!(
                            "{name} should be a positive number, got {}",
                            t.str()?
                        )));
                    }
                    Ok(t.unbind())
                }
            }
        };
        let handshake_timeout =
            timeout(ssl_handshake_timeout, SSL_HANDSHAKE_TIMEOUT, "ssl_handshake_timeout")?;
        let shutdown_timeout = timeout(ssl_shutdown_timeout, SSL_SHUTDOWN_TIMEOUT, "ssl_shutdown_timeout")?;
        let server_hostname = server_hostname.filter(|h| !h.is_none());
        let hostname_given = match &server_hostname {
            Some(h) => h.is_truthy()?,
            None => false,
        };
        let sslcontext = if sslcontext.is_truthy()? {
            sslcontext
        } else {
            // sslproto's _create_transport_context
            if server_side {
                return Err(PyValueError::new_err("Server side SSL needs a valid SSLContext"));
            }
            let ctx = s.create_default_context.bind(py).call0()?;
            if !hostname_given {
                ctx.setattr("check_hostname", false)?;
            }
            ctx
        };
        let server_hostname = if hostname_given && !server_side { server_hostname } else { None };
        let incoming = s.memory_bio.bind(py).call0()?;
        let outgoing = s.memory_bio.bind(py).call0()?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("server_side", server_side)?;
        kwargs.set_item("server_hostname", &server_hostname)?;
        let sslobj = sslcontext.call_method("wrap_bio", (&incoming, &outgoing), Some(&kwargs))?;
        let extra = PyDict::new(py);
        extra.set_item("sslcontext", &sslcontext)?;
        let core = loop_
            .getattr(intern!(py, "_core"))
            .ok()
            .and_then(|c| c.cast_into::<CoreLoop>().ok())
            .map(Bound::unbind);
        let this = SslProtocol {
            loop_: loop_.unbind(),
            core,
            server_side,
            server_hostname: server_hostname.map(Bound::unbind),
            handshake_timeout,
            shutdown_timeout,
            ssl_read: sslobj.getattr("read")?.unbind(),
            ssl_write: sslobj.getattr("write")?.unbind(),
            ssl_pending: sslobj.getattr("pending")?.unbind(),
            incoming_write: incoming.getattr("write")?.unbind(),
            outgoing_read: outgoing.getattr("read")?.unbind(),
            sslobj: sslobj.unbind(),
            incoming: incoming.unbind(),
            outgoing: outgoing.unbind(),
            sslcontext: sslcontext.unbind(),
            extra: extra.unbind(),
            state: AtomicU8::new(UNWRAPPED),
            app_state: AtomicU8::new(if call_connection_made { APP_INIT } else { APP_CON_MADE }),
            conn_lost: AtomicUsize::new(0),
            eof_received: AtomicBool::new(false),
            ssl_writing_paused: AtomicBool::new(false),
            app_reading_paused: AtomicBool::new(false),
            ssl_reading_paused: AtomicBool::new(false),
            app_writing_paused: AtomicBool::new(false),
            app_transport_created: AtomicBool::new(false),
            write_buffer_size: AtomicUsize::new(0),
            outgoing_high: Limit::new(),
            outgoing_low: Limit::new(),
            incoming_high: Limit::new(),
            incoming_low: Limit::new(),
            slots: Mutex::new(Slots {
                app: Some(App::new(&app_protocol)?),
                waiter: Some(waiter.unbind()).filter(|w| !w.is_none(py)),
                ..Slots::default()
            }),
        };
        // The default limits (what __init__'s two _set_*_limits() calls do).
        let (high, low) = flowcontrol_defaults(py, None, None, HIGH_WATER_SSL_READ)?;
        this.incoming_high.set(high.extract()?);
        this.incoming_low.set(low.extract()?);
        this.lock().incoming_limits = Some((high.unbind(), low.unbind()));
        let (high, low) = flowcontrol_defaults(py, None, None, HIGH_WATER_SSL_WRITE)?;
        this.outgoing_high.set(high.extract()?);
        this.outgoing_low.set(low.extract()?);
        this.lock().outgoing_limits = Some((high.unbind(), low.unbind()));
        Ok(this)
    }

    // -- the protocol the transport underneath calls -----------------------

    /// The connection is up: start the handshake.
    fn connection_made(slf: &Bound<'_, Self>, transport: Bound<'_, PyAny>) -> PyResult<()> {
        let py = slf.py();
        let write = transport.getattr(intern!(py, "write"))?.unbind();
        let old = {
            let mut slots = slf.get().lock();
            (slots.transport.replace(transport.unbind()), slots.transport_write.replace(write))
        };
        drop(old);
        start_handshake(slf)
    }

    #[pyo3(signature = (exc))]
    fn connection_lost(slf: &Bound<'_, Self>, exc: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        let py = slf.py();
        let this = slf.get();
        let exc = exc.filter(|e| !e.is_none());
        let backlog = std::mem::take(&mut this.lock().backlog);
        drop(backlog);
        this.outgoing_read.bind(py).call0()?;
        this.conn_lost.fetch_add(1, Relaxed);
        if let Some(t) = this.app_transport(py) {
            t.get().closed.store(true, Relaxed);
        }
        if this.state() != DO_HANDSHAKE {
            let app_state = this.app_state.load(Relaxed);
            if app_state == APP_CON_MADE || app_state == APP_EOF {
                this.app_state.store(APP_CON_LOST, Relaxed);
                let app = this.app_protocol(py).ok_or_else(|| none_has_no("connection_lost"))?;
                let lost = app.bind(py).getattr(intern!(py, "connection_lost"))?;
                this.loop_.bind(py).call_method1(intern!(py, "call_soon"), (lost, &exc))?;
            }
        }
        this.set_state(UNWRAPPED)?;
        let old = {
            let mut slots = this.lock();
            (
                slots.transport.take(),
                slots.transport_write.take(),
                slots.app_transport.take(),
                slots.app.take(),
            )
        };
        drop(old);
        wakeup_waiter(slf, exc.as_ref())?;
        let (shutdown, handshake) = {
            let mut slots = this.lock();
            (slots.shutdown_handle.take(), slots.handshake_handle.take())
        };
        Self::cancel_handle(py, shutdown)?;
        Self::cancel_handle(py, handshake)
    }

    /// Ciphertext from the peer.
    fn data_received(slf: &Bound<'_, Self>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let py = slf.py();
        let this = slf.get();
        this.incoming_write.bind(py).call1((data,))?;
        match this.state() {
            DO_HANDSHAKE => do_handshake(slf),
            WRAPPED => do_read(slf),
            FLUSHING => do_flush(slf),
            SHUTDOWN => do_shutdown(slf),
            _ => Ok(()),
        }
    }

    /// The peer closed its side of the connection underneath.
    fn eof_received(slf: &Bound<'_, Self>) -> PyResult<Option<bool>> {
        let py = slf.py();
        let this = slf.get();
        this.eof_received.store(true, Relaxed);
        // None, or True to keep the transport underneath open -- which is
        // what sslproto's own returns come to.
        let run = || -> PyResult<Option<bool>> {
            if this.debug(py)? {
                std(py)?.logger.bind(py).call_method1(intern!(py, "debug"), ("%r received EOF", slf))?;
            }
            match this.state() {
                DO_HANDSHAKE => on_handshake_complete(slf, Some(PyConnectionResetError::new_err(())))?,
                WRAPPED => {
                    this.set_state(FLUSHING)?;
                    if this.app_reading_paused.load(Relaxed) {
                        return Ok(Some(true));
                    }
                    do_flush(slf)?;
                }
                FLUSHING => {
                    do_write(slf)?;
                    this.set_state(SHUTDOWN)?;
                    do_shutdown(slf)?;
                }
                SHUTDOWN => do_shutdown(slf)?,
                _ => {}
            }
            Ok(None)
        };
        match run() {
            Err(e) if is_exception(py, &e) => {
                let transport = this.transport(py).ok_or_else(|| none_has_no("close"))?;
                transport.bind(py).call_method0(intern!(py, "close"))?;
                Err(e)
            }
            other => other,
        }
    }

    /// The transport underneath is over its high-water mark.
    fn pause_writing(&self) -> PyResult<()> {
        if self.ssl_writing_paused.swap(true, Relaxed) {
            return Err(PyAssertionError::new_err(()));
        }
        Ok(())
    }

    /// The transport underneath has drained.
    fn resume_writing(slf: &Bound<'_, Self>) -> PyResult<()> {
        if !slf.get().ssl_writing_paused.swap(false, Relaxed) {
            return Err(PyAssertionError::new_err(()));
        }
        process_outgoing(slf)
    }

    // -- callbacks this object schedules for itself -------------------------

    fn _check_handshake_timeout(slf: &Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let this = slf.get();
        if this.state() == DO_HANDSHAKE {
            let msg = format!(
                "SSL handshake is taking longer than {} seconds: aborting the connection",
                this.handshake_timeout.bind(py).str()?
            );
            fatal_error(slf, PyConnectionAbortedError::new_err(msg), "Fatal error on transport")?;
        }
        Ok(())
    }

    fn _check_shutdown_timeout(slf: &Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let this = slf.get();
        if matches!(this.state(), FLUSHING | SHUTDOWN) {
            let transport = this.transport(py).ok_or_else(|| none_has_no("_force_close"))?;
            let exc = PyTimeoutError::new_err("SSL shutdown timed out").into_value(py);
            transport.bind(py).call_method1(intern!(py, "_force_close"), (exc,))?;
        }
        Ok(())
    }

    fn _do_read(slf: &Bound<'_, Self>) -> PyResult<()> {
        do_read(slf)
    }

    fn _resume(slf: &Bound<'_, Self>) -> PyResult<()> {
        resume(slf)
    }

    // -- what sslproto's users read off it ------------------------------------

    #[getter]
    fn _app_transport(slf: &Bound<'_, Self>) -> PyResult<Option<Py<SslTransport>>> {
        // Made by sslproto's __init__; here on first ask, which is before
        // anything can have used it.
        if !slf.get().app_transport_created.load(Relaxed) {
            return get_app_transport(slf).map(Some);
        }
        Ok(slf.get().app_transport(slf.py()))
    }

    #[getter]
    fn _app_protocol(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.app_protocol(py)
    }

    #[getter]
    fn _transport(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.transport(py)
    }

    #[getter]
    fn _sslobj(&self, py: Python<'_>) -> Py<PyAny> {
        self.sslobj.clone_ref(py)
    }

    #[getter]
    fn _sslcontext(&self, py: Python<'_>) -> Py<PyAny> {
        self.sslcontext.clone_ref(py)
    }

    #[getter]
    fn _incoming(&self, py: Python<'_>) -> Py<PyAny> {
        self.incoming.clone_ref(py)
    }

    #[getter]
    fn _outgoing(&self, py: Python<'_>) -> Py<PyAny> {
        self.outgoing.clone_ref(py)
    }

    #[getter]
    fn _extra(&self, py: Python<'_>) -> Py<PyDict> {
        self.extra.clone_ref(py)
    }

    #[getter]
    fn _server_side(&self) -> bool {
        self.server_side
    }

    #[getter]
    fn _server_hostname(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.server_hostname.as_ref().map(|h| h.clone_ref(py))
    }

    #[getter]
    fn _waiter(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.lock().waiter.as_ref().map(|w| w.clone_ref(py))
    }

    /// The session's state, by sslproto's name for it.
    #[getter]
    fn _state(&self) -> &'static str {
        STATE_NAMES[self.state() as usize]
    }

    #[getter]
    fn _conn_lost(&self) -> usize {
        self.conn_lost.load(Relaxed)
    }

    fn __repr__(&self) -> String {
        format!("<cadeloop.SSLProtocol state={}>", STATE_NAMES[self.state() as usize])
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.loop_)?;
        if let Some(core) = &self.core {
            visit.call(core)?;
        }
        // A lock held elsewhere means this object is in use, not garbage.
        let Ok(slots) = self.slots.try_lock() else { return Ok(()) };
        if let Some(app) = &slots.app {
            visit.call(&app.protocol)?;
            if let Some(m) = &app.data_received {
                visit.call(m)?;
            }
            if let Some((g, u)) = &app.buffered {
                visit.call(g)?;
                visit.call(u)?;
            }
        }
        if let Some(t) = &slots.app_transport {
            visit.call(t)?;
        }
        for obj in [
            &slots.transport,
            &slots.transport_write,
            &slots.waiter,
            &slots.handshake_handle,
            &slots.shutdown_handle,
        ]
        .into_iter()
        .flatten()
        {
            visit.call(obj)?;
        }
        Ok(())
    }

    fn __clear__(&self) {
        let slots = std::mem::take(&mut *self.lock());
        drop(slots);
    }
}

/// asyncio's `sslproto._SSLProtocolTransport`: the transport of a TLS
/// connection, as the application's protocol sees it.
#[pyclass(frozen, weakref, module = "cadeloop._core", name = "SSLTransport")]
pub struct SslTransport {
    loop_: Py<PyAny>,
    ssl_protocol: Mutex<Option<Py<SslProtocol>>>,
    closed: AtomicBool,
    /// A close() made on another thread is on its way to the loop's:
    /// `is_closing()` is true from then, as it is after any close().
    close_handed: AtomicBool,
}

impl SslTransport {
    fn protocol_slot(&self) -> MutexGuard<'_, Option<Py<SslProtocol>>> {
        self.ssl_protocol.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `self._ssl_protocol`, or the AttributeError that using it after a
    /// second close() gives in sslproto.
    fn protocol<'py>(&self, py: Python<'py>, attr: &str) -> PyResult<Bound<'py, SslProtocol>> {
        match self.protocol_slot().as_ref() {
            Some(p) => Ok(p.bind(py).clone()),
            None => Err(none_has_no(attr)),
        }
    }

    /// A call made off the loop's thread, or from inside the loop's own
    /// state (a finalizer run by an allocation there): hand it to the
    /// loop's thread (ADR-36). True if it was handed over.
    fn handed(slf: &Bound<'_, Self>, method: &str, arg: Option<Py<PyAny>>) -> PyResult<bool> {
        let py = slf.py();
        let core = match slf.get().protocol_slot().as_ref() {
            Some(p) => p.get().core.as_ref().map(|c| c.clone_ref(py)),
            None => None,
        };
        let Some(core) = core else { return Ok(false) };
        let core = core.bind(py).get();
        if !core.belongs_on_loop_thread() {
            return Ok(false);
        }
        let callback = slf.getattr(method)?;
        let args = match arg {
            Some(arg) => CallArgs::One(arg),
            None => CallArgs::Zero,
        };
        core.hand_over(py, &callback, args)?;
        Ok(true)
    }
}

#[pymethods]
impl SslTransport {
    #[classattr]
    fn _start_tls_compatible() -> bool {
        true
    }

    #[classattr]
    fn _sendfile_compatible(py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(std(py)?.sendfile_fallback.clone_ref(py))
    }

    #[pyo3(signature = (name, default=None))]
    fn get_extra_info(
        &self,
        py: Python<'_>,
        name: Bound<'_, PyAny>,
        default: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let proto = self.protocol(py, "_get_extra_info")?;
        let this = proto.get();
        if let Some(value) = this.extra.bind(py).get_item(&name)? {
            return Ok(value.unbind());
        }
        match this.transport(py) {
            Some(transport) => {
                Ok(transport.bind(py).call_method1(intern!(py, "get_extra_info"), (name, default))?.unbind())
            }
            None => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    fn set_protocol(&self, py: Python<'_>, protocol: Bound<'_, PyAny>) -> PyResult<()> {
        let proto = self.protocol(py, "_set_app_protocol")?;
        let app = App::new(&protocol)?;
        let old = proto.get().lock().app.replace(app);
        drop(old);
        Ok(())
    }

    fn get_protocol(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        Ok(self.protocol(py, "_app_protocol")?.get().app_protocol(py))
    }

    /// sslproto's `_SSLProtocolTransport.is_closing()`. From 3.12 it is
    /// also true once the transport underneath is closing -- a peer's
    /// reset, an abort -- and not only after this one's own close().
    fn is_closing(&self, py: Python<'_>) -> PyResult<bool> {
        if self.closed.load(Relaxed) || self.close_handed.load(Relaxed) {
            return Ok(true);
        }
        #[cfg(Py_3_12)]
        {
            // The protocol is taken out of its slot before anything is
            // called: no lock is held across Python.
            let proto = self.protocol_slot().as_ref().map(|p| p.clone_ref(py));
            if let Some(transport) = proto.and_then(|p| p.get().transport(py)) {
                return transport.bind(py).call_method0(intern!(py, "is_closing"))?.is_truthy();
            }
        }
        #[cfg(not(Py_3_12))]
        let _ = py;
        Ok(false)
    }

    /// Flush what is buffered, exchange close_notify alerts, close.
    fn close(slf: &Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let this = slf.get();
        if !this.closed.load(Relaxed) && Self::handed(slf, "close", None)? {
            this.close_handed.store(true, Relaxed);
            return Ok(());
        }
        if !this.closed.swap(true, Relaxed) {
            let proto = this.protocol(py, "_start_shutdown")?;
            start_shutdown(&proto)
        } else {
            let old = this.protocol_slot().take();
            drop(old);
            Ok(())
        }
    }

    fn is_reading(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(!self.protocol(py, "_app_reading_paused")?.get().app_reading_paused.load(Relaxed))
    }

    fn pause_reading(&self, py: Python<'_>) -> PyResult<()> {
        self.protocol(py, "_pause_reading")?.get().app_reading_paused.store(true, Relaxed);
        Ok(())
    }

    fn resume_reading(&self, py: Python<'_>) -> PyResult<()> {
        let proto = self.protocol(py, "_resume_reading")?;
        let this = proto.get();
        if this.app_reading_paused.swap(false, Relaxed) {
            this.loop_
                .bind(py)
                .call_method1(intern!(py, "call_soon"), (proto.getattr(intern!(py, "_resume"))?,))?;
        }
        Ok(())
    }

    #[pyo3(signature = (high=None, low=None))]
    fn set_write_buffer_limits(
        &self,
        py: Python<'_>,
        high: Option<Bound<'_, PyAny>>,
        low: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let proto = self.protocol(py, "_set_write_buffer_limits")?;
        set_write_buffer_limits(&proto, high, low)?;
        control_app_writing(&proto, false)
    }

    fn get_write_buffer_limits(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let proto = self.protocol(py, "_outgoing_low_water")?;
        let slots = proto.get().lock();
        let (high, low) = slots.outgoing_limits.as_ref().ok_or_else(|| none_has_no("_outgoing_low_water"))?;
        Ok((low.clone_ref(py), high.clone_ref(py)))
    }

    fn get_write_buffer_size(&self, py: Python<'_>) -> PyResult<usize> {
        get_write_buffer_size(&self.protocol(py, "_get_write_buffer_size")?)
    }

    #[pyo3(signature = (high=None, low=None))]
    fn set_read_buffer_limits(
        &self,
        py: Python<'_>,
        high: Option<Bound<'_, PyAny>>,
        low: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let proto = self.protocol(py, "_set_read_buffer_limits")?;
        set_read_buffer_limits(&proto, high, low)?;
        control_ssl_reading(&proto)
    }

    fn get_read_buffer_limits(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, Py<PyAny>)> {
        let proto = self.protocol(py, "_incoming_low_water")?;
        let slots = proto.get().lock();
        let (high, low) = slots.incoming_limits.as_ref().ok_or_else(|| none_has_no("_incoming_low_water"))?;
        Ok((low.clone_ref(py), high.clone_ref(py)))
    }

    fn get_read_buffer_size(&self, py: Python<'_>) -> PyResult<usize> {
        get_read_buffer_size(&self.protocol(py, "_get_read_buffer_size")?)
    }

    #[getter]
    fn _protocol_paused(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.protocol(py, "_app_writing_paused")?.get().app_writing_paused.load(Relaxed))
    }

    #[getter]
    fn _ssl_protocol(&self, py: Python<'_>) -> Option<Py<SslProtocol>> {
        self.protocol_slot().as_ref().map(|p| p.clone_ref(py))
    }

    #[getter]
    fn _closed(&self) -> bool {
        self.closed.load(Relaxed)
    }

    #[getter]
    fn _loop(&self, py: Python<'_>) -> Py<PyAny> {
        self.loop_.clone_ref(py)
    }

    /// Encrypt and send. Buffers, and arranges for the sending, without
    /// blocking.
    fn write(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<()> {
        if !(data.is_instance_of::<PyBytes>()
            || data.is_instance_of::<PyByteArray>()
            || data.is_instance_of::<PyMemoryView>())
        {
            return Err(PyTypeError::new_err(format!(
                "data: expecting a bytes-like instance, got {}",
                data.get_type().name()?
            )));
        }
        if py_len(data)? == 0 {
            return Ok(());
        }
        let proto = self.protocol(py, "_write_appdata")?;
        write_appdata(&proto, PyTuple::new(py, [data])?.as_any())
    }

    fn writelines(&self, py: Python<'_>, list_of_data: &Bound<'_, PyAny>) -> PyResult<()> {
        let proto = self.protocol(py, "_write_appdata")?;
        write_appdata(&proto, list_of_data)
    }

    /// sslproto's test hook, which CPython's own tests (and this
    /// repository's) use to put data behind a handshake or a shutdown.
    #[pyo3(name = "_test__append_write_backlog")]
    fn test_append_write_backlog(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let proto = self.protocol(py, "_write_backlog")?;
        let len = py_len(&data)?;
        proto.get().lock().backlog.push_back(data.unbind());
        proto.get().write_buffer_size.fetch_add(len, Relaxed);
        Ok(())
    }

    /// TLS has no half-close.
    fn write_eof(&self) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(()))
    }

    fn can_write_eof(&self) -> bool {
        false
    }

    /// Close at once: what is buffered is lost.
    fn abort(slf: &Bound<'_, Self>) -> PyResult<()> {
        Self::_force_close(slf, None)
    }

    #[pyo3(signature = (exc))]
    fn _force_close(slf: &Bound<'_, Self>, exc: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        let py = slf.py();
        let this = slf.get();
        let exc = exc.filter(|e| !e.is_none());
        let arg = Some(exc.as_ref().map_or_else(|| py.None(), |e| e.clone().unbind()));
        if Self::handed(slf, "_force_close", arg)? {
            this.closed.store(true, Relaxed);
            return Ok(());
        }
        this.closed.store(true, Relaxed);
        let proto = this.protocol_slot().as_ref().map(|p| p.bind(py).clone());
        match proto {
            Some(proto) => abort(&proto, exc.as_ref()),
            None => Ok(()),
        }
    }

    fn __repr__(&self) -> String {
        format!("<cadeloop.SSLTransport closing={}>", self.closed.load(Relaxed))
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.loop_)?;
        if let Ok(slot) = self.ssl_protocol.try_lock() {
            if let Some(p) = slot.as_ref() {
                visit.call(p)?;
            }
        }
        Ok(())
    }

    fn __clear__(&self) {
        let old = self.protocol_slot().take();
        drop(old);
    }
}

impl Drop for SslTransport {
    /// sslproto's `_SSLProtocolTransport.__del__`: a transport collected
    /// without having been closed says so.
    fn drop(&mut self) {
        if *self.closed.get_mut() {
            return;
        }
        Python::attach(|py| {
            // Through the C API: importing `warnings` here fails once the
            // interpreter is shutting down, which is when the last
            // unclosed transports are collected.
            let category = py.get_type::<pyo3::exceptions::PyResourceWarning>();
            let message = c"unclosed transport <asyncio._SSLProtocolTransport object>";
            if let Err(e) = PyErr::warn(py, &category, message, 1) {
                e.write_unraisable(py, None);
            }
        });
    }
}
