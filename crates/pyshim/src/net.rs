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

use cadeloop_core::backend::{is_cancelled_error, Completion, IoBackend, IoSlice, RawSocket};
use cadeloop_core::buffers::{BufferPool, SizeClass, SlotId};
use cadeloop_core::netsys;
use cadeloop_core::opslab::OpId;
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::coreloop::CoreLoop;

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
    Recv { tid: u64, slot: SlotId },
    Send(u64),
    Connect { fut: Py<PyAny>, sock: RawSocket },
    Accept(u64),
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

pub(crate) struct TransportEntry {
    pub socket: RawSocket,
    pub proto: ProtoRefs,
    pub pyobj: Py<Transport>,
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
}

pub(crate) struct ListenerEntry {
    pub socket: RawSocket,
    pub factory: Py<PyAny>,
    accept_ops: Vec<OpId>,
    target: usize,
    closing: bool,
}

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
    pub graveyard_entries: Vec<TransportEntry>,
    pub graveyard_bufs: Vec<WriteBuf>,
    pub graveyard_py: Vec<Py<PyAny>>,
    pub stats_bytes_rx: u64,
    pub stats_bytes_tx: u64,
    pub stats_conns_accepted: u64,
}

// SAFETY: thread-affine by the gil_boundary protocol — only the owner
// thread touches NetState (raw pointers into retained buffers / pool slabs
// never cross threads).
unsafe impl Send for NetState {}

impl NetState {
    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
}

/// Phase-2 events: dispatched with the GIL, outside the state cell.
pub(crate) enum NetEvent {
    /// Plain-protocol data: payload prebuilt, slot already re-posted.
    Data {
        data_received: Py<PyAny>,
        payload: Py<PyAny>,
    },
    /// BufferedProtocol data: copy out of the retained slot in phase 2
    /// (get_buffer may run arbitrary Python, so it cannot run in-cell).
    BufData {
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
}

// --------------------------------------------------------------------- //
// in-cell machinery (no user Python, no Py drops)                       //
// --------------------------------------------------------------------- //

type Backend<'a> = &'a mut (dyn IoBackend + Send);

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
    if entry.conn_lost {
        net.graveyard_entries.push(entry);
        return;
    }
    entry.conn_lost = true;
    for op in [entry.recv_op.take(), entry.send_op.take()].into_iter().flatten() {
        let _ = backend.cancel(op);
        net.ops.remove(&op);
    }
    backend.detach_socket(entry.socket);
    netsys::close(entry.socket);
    if let Some(slot) = entry.recv_slot.take() {
        net.buffers.release(slot);
    }
    net.events.push(NetEvent::ConnLost { connection_lost: entry.proto.connection_lost.clone_ref(py), err });
    net.graveyard_entries.push(entry);
}

/// Used by CoreLoop::close (no Python available for events — they are
/// cleared right after).
pub(crate) fn teardown(net: &mut NetState, backend: Backend<'_>, tid: u64, _err: Option<u32>) {
    let Some(mut entry) = net.transports.remove(&tid) else { return };
    entry.conn_lost = true;
    for op in [entry.recv_op.take(), entry.send_op.take()].into_iter().flatten() {
        let _ = backend.cancel(op);
        net.ops.remove(&op);
    }
    backend.detach_socket(entry.socket);
    netsys::close(entry.socket);
    if let Some(slot) = entry.recv_slot.take() {
        net.buffers.release(slot);
    }
    net.graveyard_entries.push(entry);
}

/// Post the next recv on a transport. In-cell.
fn post_recv(py: Python<'_>, net: &mut NetState, backend: Backend<'_>, tid: u64) {
    let Some(entry) = net.transports.get_mut(&tid) else { return };
    if entry.recv_op.is_some() || entry.reading_paused || entry.closing || entry.conn_lost {
        return;
    }
    let slot = match entry.recv_slot {
        Some(s) => s,
        None => {
            let s = net.buffers.acquire(RECV_CLASS);
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

pub(crate) fn listener_create(net: &mut NetState, sock: RawSocket, factory: Py<PyAny>, target: usize) -> u64 {
    let lid = net.next_id();
    net.listeners.insert(
        lid,
        ListenerEntry {
            socket: sock,
            factory,
            accept_ops: Vec::new(),
            target: target.max(1),
            closing: false,
        },
    );
    lid
}

pub(crate) fn listener_start(net: &mut NetState, backend: Backend<'_>, lid: u64) {
    post_accepts(net, backend, lid);
}

pub(crate) fn listener_teardown(net: &mut NetState, backend: Backend<'_>, lid: u64) {
    let Some(mut listener) = net.listeners.remove(&lid) else { return };
    listener.closing = true;
    for op in listener.accept_ops.drain(..) {
        let _ = backend.cancel(op);
        net.ops.remove(&op);
    }
    backend.detach_socket(listener.socket);
    netsys::close(listener.socket);
    net.graveyard_py.push(listener.factory);
}

fn post_accepts(net: &mut NetState, backend: Backend<'_>, lid: u64) {
    loop {
        let Some(listener) = net.listeners.get_mut(&lid) else { return };
        if listener.closing || listener.accept_ops.len() >= listener.target {
            return;
        }
        let socket = listener.socket;
        match backend.post_accept(socket) {
            Ok(op) => {
                net.listeners.get_mut(&lid).unwrap().accept_ops.push(op);
                net.ops.insert(op, OpTarget::Accept(lid));
            }
            Err(_) => return, // transient (e.g. fd limit): retried on next completion
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
        // Peer EOF. Recv is NOT re-posted; phase 2 decides close-vs-keep
        // from eof_received()'s return value.
        net.events.push(NetEvent::Eof {
            eof_received: entry.proto.eof_received.clone_ref(py),
            transport: entry.pyobj.clone_ref(py),
        });
        return;
    }
    net.stats_bytes_rx += bytes as u64;
    debug_assert_eq!(entry.recv_slot, Some(slot), "op slot / transport slot mismatch");
    if let Some(data_received) = &entry.proto.data_received {
        // Plain Protocol: materialize bytes now (non-GC-tracked allocation,
        // safe in-cell) and re-post into the SAME slot immediately so the
        // kernel refills while Python processes this chunk.
        let data_received = data_received.clone_ref(py);
        let ptr = net.buffers.slot_ptr(slot);
        let payload = unsafe {
            let obj = ffi::PyBytes_FromStringAndSize(ptr.cast(), bytes as ffi::Py_ssize_t);
            Py::from_owned_ptr(py, obj)
        };
        net.events.push(NetEvent::Data { data_received, payload });
        post_recv(py, net, backend, tid);
    } else {
        // BufferedProtocol: the transport's slot reference transfers to
        // the phase-2 event (released there after the copy); a fresh slot
        // is acquired for the next read.
        let get_buffer = entry.proto.get_buffer.as_ref().unwrap().clone_ref(py);
        let buffer_updated = entry.proto.buffer_updated.as_ref().unwrap().clone_ref(py);
        entry.recv_slot = None; // ref now owned by the BufData event
        net.events.push(NetEvent::BufData { get_buffer, buffer_updated, slot, len: bytes as usize });
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
    if entry.proto_paused && entry.queued_bytes <= entry.low_water {
        entry.proto_paused = false;
        let resume_writing = entry.proto.resume_writing.clone_ref(py);
        net.events.push(NetEvent::ResumeWriting { resume_writing });
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
        if !is_cancelled_error(os_error) && !closing {
            net.events.push(NetEvent::AcceptError { err: os_error });
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
            NetEvent::Data { data_received, payload } => {
                core.guard_protocol_call(py, data_received.call1(py, (payload,)))?;
            }
            NetEvent::BufData { get_buffer, buffer_updated, slot, len } => {
                let res = fill_app_buffer(py, core, &get_buffer, &buffer_updated, slot, len);
                core.with_net(|net, _| {
                    net.buffers.release(slot);
                })?;
                core.guard_protocol_call(py, res)?;
            }
            NetEvent::Eof { eof_received, transport } => match eof_received.call0(py) {
                Ok(keep_open) => {
                    if !keep_open.is_truthy(py).unwrap_or(false) {
                        transport.bind(py).get().close(py)?;
                    }
                }
                Err(e) => core.guard_protocol_call::<Py<PyAny>>(py, Err(e))?,
            },
            NetEvent::ConnLost { connection_lost, err } => {
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
                let factory =
                    core.with_net(|net, _| net.listeners.get(&lid).map(|l| l.factory.clone_ref(py)))?;
                let Some(factory) = factory else {
                    netsys::close(sock);
                    continue;
                };
                match factory.call0(py) {
                    Ok(protocol) => {
                        if let Err(e) = wire_stream(py, slf, sock, protocol.into_bound(py)) {
                            core.guard_protocol_call::<Py<PyAny>>(py, Err(e))?;
                        }
                    }
                    Err(e) => {
                        netsys::close(sock);
                        core.guard_protocol_call::<Py<PyAny>>(py, Err(e))?;
                    }
                }
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
        }
    }
    Ok(())
}

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

fn addr_tuple(py: Python<'_>, addr: Option<std::net::SocketAddr>) -> Option<Py<PyAny>> {
    addr.map(|a| {
        let ip = a.ip().to_string();
        let port = a.port();
        (ip, port).into_pyobject(py).unwrap().into_any().unbind()
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
    let proto = match cache_proto(py, &protocol) {
        Ok(p) => p,
        Err(e) => {
            netsys::close(sock);
            return Err(e);
        }
    };
    let _ = netsys::set_nodelay(sock, true); // R-038
    let peer = addr_tuple(py, netsys::peername(sock).ok());
    let name = addr_tuple(py, netsys::sockname(sock).ok());
    let connection_made = protocol.getattr("connection_made")?;

    let (high, low) = core.water_marks();
    let reg = core.with_net(|_net, reactor| reactor.backend_mut().register_socket(sock))?;
    if let Err(e) = reg {
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
                proto,
                pyobj: transport.clone_ref(py),
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
}

#[pymethods]
impl Transport {
    // ---- write path ---------------------------------------------------

    fn write(&self, py: Python<'_>, data: Bound<'_, PyAny>) -> PyResult<()> {
        let core = self.core_ref(py);
        // R-074 retention: exact bytes -> zero-copy retain; any other
        // buffer exporter -> copy now.
        let buf: WriteBuf = if let Ok(b) = data.downcast::<PyBytes>() {
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
            if entry.closing || entry.eof_wanted || entry.conn_lost {
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
                    pause = Some(entry.proto.pause_writing.clone_ref(py));
                }
            }
            dirty |= !net.graveyard_bufs.is_empty() || !net.graveyard_entries.is_empty();
        })?;
        if dirty {
            // Rare paths only (dropped write, >=64KiB flush that tore down).
            core.drain_graveyards(py)?;
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
        let high = high.unwrap_or(64 * 1024);
        let low = low.unwrap_or(high / 4);
        if low > high {
            return Err(pyo3::exceptions::PyValueError::new_err("high must be >= low must be >= 0"));
        }
        core.with_net(|net, _| {
            if let Some(e) = net.transports.get_mut(&self.tid) {
                e.high_water = high;
                e.low_water = low;
            }
        })
    }

    // ---- read path -----------------------------------------------------

    fn pause_reading(&self, py: Python<'_>) -> PyResult<()> {
        let core = self.core_ref(py);
        let tid = self.tid;
        let _ = tid;
        core.with_net(|net, _reactor| {
            if let Some(entry) = net.transports.get_mut(&tid) {
                // The in-flight recv (if any) is left to complete and its
                // data is delivered (it already left the kernel buffer) —
                // it is simply not re-posted. Cancelling here creates
                // completion/slot-reuse races and costs syscalls; asyncio's
                // pause_reading is advisory, so late delivery is conformant.
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
                    if entry.recv_op.is_none() {
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
                let old = std::mem::replace(&mut entry.proto, refs);
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
            } else {
                net.graveyard_py.push(refs.protocol);
            }
        })?;
        core.drain_graveyards(py)
    }

    fn get_protocol(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let core = self.core_ref(py);
        core.with_net(|net, _| {
            net.transports.get(&self.tid).map(|e| e.proto.protocol.clone_ref(py)).unwrap_or_else(|| py.None())
        })
    }

    #[pyo3(signature = (name, default=None))]
    fn get_extra_info(&self, py: Python<'_>, name: &str, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        let core = self.core_ref(py);
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
