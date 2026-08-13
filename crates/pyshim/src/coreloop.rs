//! `CoreLoop`: the native scheduling + I/O core behind `cadeloop.Loop`.
//!
//! Owns the L1 reactor and the L2 net state (transports, listeners,
//! watches). Implements the native fast paths of R-050. The Python facade
//! (`python/cadeloop/loop.py`) completes the `asyncio.AbstractEventLoop`
//! surface (R-013) on top.
//!
//! Tick shape (M1): pending corked writes flush BEFORE the park so no
//! written byte ever waits out a poll; protocol-callback writes flush in
//! the same tick they were produced:
//!
//! ```text
//! 1. flush corked writes queued by last tick's ready callbacks
//!    prepare_tick; poll (GIL released, R-021)
//! 2. translate completions -> NetEvents (in-cell, no Python execution)
//! 3. dispatch NetEvents (protocol callbacks; GIL, out-of-cell)
//! 4. flush writes those callbacks corked (R-035 tick-end flush)
//! 5. dispatch NetEvents produced by the flush (teardowns)
//! 6. dispatch ready-callback batch (<=128, R-054)
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use cadeloop_core::backend::{BackendKind, RawSocket, Wakeup};
use cadeloop_core::netsys;
use cadeloop_core::reactor::{Reactor, ReactorConfig};
use cadeloop_core::ready::CrossThreadQueue;
use cadeloop_core::time::{secs_f64_to_ticks, ticks_to_secs_f64, Ticks};
use cadeloop_core::timer::TimerToken;
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError, PySystemExit, PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::gil_boundary::StateCell;
use crate::handles::{run_handle, DispatchOutcome, Handle, TimerHandle};
use crate::net::{self, NetEvent, NetState, Transport};

pub(crate) struct LoopState {
    pub reactor: Reactor<Py<PyAny>>,
    pub net: NetState,
    /// Scratch for per-tick completion copies (reused, no per-tick alloc).
    completions_scratch: Vec<cadeloop_core::backend::Completion>,
}

#[pyclass(frozen, module = "cadeloop._core")]
pub struct CoreLoop {
    state: StateCell<LoopState>,
    xqueue: Arc<CrossThreadQueue<Py<PyAny>>>,
    waker: Arc<dyn Wakeup>,
    timer_cancels: Arc<AtomicUsize>,
    closed: AtomicBool,
    stopping: AtomicBool,
    debug: AtomicBool,
    /// `loop.time()` cache, updated once per tick (R-061), readable from
    /// any thread without touching loop state.
    cached_time_ns: AtomicU64,
    /// Tick counter for the throttled signal check.
    tick_no: AtomicU64,
    /// Facade hook: `(handle, exception) -> None` for callback errors.
    error_hook: OnceLock<Py<PyAny>>,
    /// Facade hook: `(message, exception) -> None` for protocol/net errors.
    net_error_hook: OnceLock<Py<PyAny>>,
    /// Facade hook: `(handle, seconds) -> None` (R-142 slow callbacks).
    slow_callback_hook: OnceLock<Py<PyAny>>,
    backend_name: &'static str,
    high_water: usize,
    low_water: usize,
}

impl CoreLoop {
    fn check_closed(&self) -> PyResult<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(PyRuntimeError::new_err("Event loop is closed"))
        } else {
            Ok(())
        }
    }

    fn now_ns(&self) -> PyResult<Ticks> {
        if self.state.is_claimed() {
            Ok(self.cached_time_ns.load(Ordering::Acquire))
        } else {
            self.state.with(|st| st.reactor.now_fresh())
        }
    }

    /// Exclusive access to (net, reactor) — see gil_boundary contract.
    pub(crate) fn with_net<R>(
        &self,
        f: impl FnOnce(&mut NetState, &mut Reactor<Py<PyAny>>) -> R,
    ) -> PyResult<R> {
        self.state.with(|st| f(&mut st.net, &mut st.reactor))
    }

    pub(crate) fn water_marks(&self) -> (usize, usize) {
        (self.high_water, self.low_water)
    }

    /// Drop graveyarded Python refs / buffers outside the state cell.
    pub(crate) fn drain_graveyards(&self, _py: Python<'_>) -> PyResult<()> {
        let (entries, bufs, pys, timers) = self.state.with(|st| {
            (
                std::mem::take(&mut st.net.graveyard_entries),
                std::mem::take(&mut st.net.graveyard_bufs),
                std::mem::take(&mut st.net.graveyard_py),
                st.reactor.take_graveyard(),
            )
        })?;
        drop((entries, bufs, pys, timers));
        Ok(())
    }

    /// Route a non-fatal protocol/callback exception to the facade's
    /// exception handler; propagate KeyboardInterrupt/SystemExit.
    pub(crate) fn guard_protocol_call<T>(&self, py: Python<'_>, res: PyResult<T>) -> PyResult<()> {
        match res {
            Ok(_) => Ok(()),
            Err(e) if e.is_instance_of::<PyKeyboardInterrupt>(py) || e.is_instance_of::<PySystemExit>(py) => {
                Err(e)
            }
            Err(e) => {
                self.report_net_error(
                    py,
                    "Exception in network protocol callback",
                    e.into_value(py).into_any(),
                );
                Ok(())
            }
        }
    }

    pub(crate) fn report_net_error(&self, py: Python<'_>, message: &str, exc: Py<PyAny>) {
        match self.net_error_hook.get() {
            Some(hook) => {
                if let Err(e) = hook.call1(py, (message, &exc)) {
                    e.write_unraisable(py, None);
                }
            }
            None => {
                let err = PyRuntimeError::new_err(message.to_string());
                err.write_unraisable(py, Some(exc.bind(py)));
            }
        }
    }

    fn report_failure(&self, py: Python<'_>, handle: &Py<PyAny>, err: PyErr) {
        match self.error_hook.get() {
            Some(hook) => {
                if let Err(hook_err) = hook.call1(py, (handle, err.value(py))) {
                    hook_err.write_unraisable(py, Some(handle.bind(py)));
                }
            }
            None => err.write_unraisable(py, Some(handle.bind(py))),
        }
    }

    /// Build a Handle (created OUTSIDE the cell: allocation can GC).
    pub(crate) fn make_handle(
        &self,
        py: Python<'_>,
        callback: &Bound<'_, PyAny>,
        args: &Bound<'_, PyTuple>,
        context: Option<&Bound<'_, PyAny>>,
        method: &str,
    ) -> PyResult<Handle> {
        if !callback.is_callable() {
            return Err(PyTypeError::new_err(format!(
                "a callable object was expected by {method}(), got {}",
                callback.repr()?
            )));
        }
        let context: Py<PyAny> = match context {
            Some(c) if !c.is_none() => c.clone().unbind(),
            _ => copy_context(py)?,
        };
        Ok(Handle::new(callback.clone().unbind(), args.clone().unbind(), context))
    }

    /// Flush transports whose writes were corked (R-035). Returns pending
    /// net events produced by the flush (teardowns).
    fn flush_corked(&self, py: Python<'_>) -> PyResult<Vec<NetEvent>> {
        self.state.with(|st| {
            let list = std::mem::take(&mut st.net.flush_list);
            for tid in list {
                net::flush_pending(py, &mut st.net, st.reactor.backend_mut(), tid);
            }
            std::mem::take(&mut st.net.events)
        })
    }

    /// One full tick — see module docs for the phase ordering.
    fn tick(&self, slf: &Bound<'_, CoreLoop>, py: Python<'_>) -> PyResult<()> {
        let stopping = self.stopping.load(Ordering::Acquire);
        // Phase 1: flush corked writes from last tick's callbacks, then poll.
        let (poll_result, parked): (std::io::Result<()>, bool) = self.state.with(|st| {
            if !st.net.flush_list.is_empty() {
                let list = std::mem::take(&mut st.net.flush_list);
                for tid in list {
                    net::flush_pending(py, &mut st.net, st.reactor.backend_mut(), tid);
                }
            }
            st.reactor.prepare_tick();
            self.cached_time_ns.store(st.reactor.time_cached(), Ordering::Release);
            let timeout = if stopping || !st.net.events.is_empty() {
                Duration::ZERO
            } else {
                st.reactor.poll_timeout()
            };
            let reactor = &mut st.reactor;
            if timeout.is_zero() {
                // Non-blocking reap: keeping the GIL is legal (nothing can
                // block) and skips a save/restore of the thread state.
                (reactor.poll(timeout), false)
            } else {
                // R-021: the only GIL release point; sound because `claim`
                // guarantees no other thread can enter this state.
                (py.allow_threads(move || reactor.poll(timeout)), true)
            }
        })?;
        poll_result?;

        // Ctrl-C responsiveness: a parked poll returns promptly on EINTR, so
        // check right after it; busy (non-parked) ticks check every 64th
        // tick (~tens of microseconds) instead of paying the call per tick.
        let tick_no = self.tick_no.fetch_add(1, Ordering::Relaxed);
        if parked || tick_no & 63 == 0 {
            unsafe {
                if ffi::PyErr_CheckSignals() != 0 {
                    return Err(PyErr::fetch(py));
                }
            }
        }

        // Phase 2: translate completions in-cell; collect events/graveyards
        // in ONE cell entry (`graveyard` items must drop out-of-cell).
        let (events, graveyard) = self.state.with(|st| {
            st.reactor.finish_poll_after(parked);
            if parked {
                self.cached_time_ns.store(st.reactor.time_cached(), Ordering::Release);
            }
            let mut comps = std::mem::take(&mut st.completions_scratch);
            st.reactor.drain_completions(&mut comps);
            if !comps.is_empty() {
                net::translate(py, &mut st.net, st.reactor.backend_mut(), &comps);
                comps.clear();
            }
            st.completions_scratch = comps;
            // Readiness watch callbacks join the ordinary ready queue.
            if !st.net.ready_scratch.is_empty() {
                let ready: Vec<Py<PyAny>> = std::mem::take(&mut st.net.ready_scratch);
                for h in ready {
                    st.reactor.push_ready(h);
                }
            }
            (std::mem::take(&mut st.net.events), take_graveyards(st))
        })?;
        drop(graveyard);

        // Phases 3-5 only exist when network events fired: dispatch protocol
        // callbacks, flush the writes they corked (same tick, R-035), then
        // dispatch teardowns produced by that flush. Writes made by plain
        // ready callbacks (phase 6) flush at the next tick's phase 1,
        // before any park. Pure-scheduling ticks skip all of this.
        if !events.is_empty() {
            net::dispatch_events(py, slf, events)?;
            let events = self.flush_corked(py)?;
            self.drain_graveyards(py)?;
            if !events.is_empty() {
                net::dispatch_events(py, slf, events)?;
            }
        }

        // Phase 6: ready-callback batch (R-054) — taken from the cell in
        // ONE entry (rloop/rsloop batch-swap pattern); the buffer lives in
        // loop-thread TLS and is always drained before being parked again.
        DISPATCH_BUF.with_borrow_mut(|batch| -> PyResult<()> {
            debug_assert!(batch.is_empty());
            self.state.with(|st| {
                while let Some(token) = st.reactor.pop_ready_batched() {
                    batch.push(token);
                }
            })?;
            let debug = self.debug.load(Ordering::Relaxed);
            for token in batch.drain(..) {
                let started = debug.then(std::time::Instant::now);
                match run_handle(py, token.bind(py))? {
                    DispatchOutcome::Done => {}
                    DispatchOutcome::Failed(err) => self.report_failure(py, &token, err),
                }
                if let Some(started) = started {
                    let elapsed = started.elapsed();
                    if elapsed > Duration::from_millis(100) {
                        if let Some(hook) = self.slow_callback_hook.get() {
                            if let Err(e) = hook.call1(py, (&token, elapsed.as_secs_f64())) {
                                e.write_unraisable(py, Some(token.bind(py)));
                            }
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

thread_local! {
    /// Reusable dispatch buffer (loop thread only; empty between ticks).
    static DISPATCH_BUF: std::cell::RefCell<Vec<Py<PyAny>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

type Graveyards =
    (Vec<crate::net::TransportEntry>, Vec<crate::net::WriteBuf>, Vec<Py<PyAny>>, Vec<Py<PyAny>>);

fn take_graveyards(st: &mut LoopState) -> Graveyards {
    (
        std::mem::take(&mut st.net.graveyard_entries),
        std::mem::take(&mut st.net.graveyard_bufs),
        std::mem::take(&mut st.net.graveyard_py),
        st.reactor.take_graveyard(),
    )
}

fn copy_context(py: Python<'_>) -> PyResult<Py<PyAny>> {
    unsafe {
        let ptr = ffi::PyContext_CopyCurrent();
        if ptr.is_null() {
            return Err(PyErr::fetch(py));
        }
        Ok(Py::from_owned_ptr(py, ptr))
    }
}

#[pymethods]
impl CoreLoop {
    #[new]
    #[pyo3(signature = (backend="auto", spin_us=20, high_water=65536, low_water=16384))]
    fn new(backend: &str, spin_us: u64, high_water: usize, low_water: usize) -> PyResult<Self> {
        let kind = BackendKind::parse(backend).ok_or_else(|| {
            PyValueError::new_err(format!("invalid backend {backend:?}: expected 'auto', 'iocp' or 'rio'"))
        })?;
        let cfg = ReactorConfig { backend: kind, spin_us, ..Default::default() };
        let reactor: Reactor<Py<PyAny>> = Reactor::new(cfg)?;
        let (xqueue, waker) = reactor.cross_thread_handles();
        let timer_cancels = reactor.timer_cancel_counter();
        let backend_name = reactor.backend_name();
        Ok(CoreLoop {
            state: StateCell::new(LoopState {
                reactor,
                net: NetState::default(),
                completions_scratch: Vec::with_capacity(512),
            }),
            xqueue,
            waker,
            timer_cancels,
            closed: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            debug: AtomicBool::new(false),
            cached_time_ns: AtomicU64::new(0),
            tick_no: AtomicU64::new(0),
            error_hook: OnceLock::new(),
            net_error_hook: OnceLock::new(),
            slow_callback_hook: OnceLock::new(),
            backend_name,
            high_water,
            low_water,
        })
    }

    // ---- lifecycle -------------------------------------------------------

    fn run_forever(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let this = slf.get();
        this.check_closed()?;
        let guard = this.state.claim()?;
        let result = loop {
            match this.tick(slf, py) {
                Err(e) => break Err(e),
                Ok(()) => {
                    if this.stopping.load(Ordering::Acquire) {
                        break Ok(());
                    }
                }
            }
        };
        this.stopping.store(false, Ordering::Release);
        drop(guard);
        result
    }

    fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        self.waker.wake();
    }

    fn is_running(&self) -> bool {
        self.state.is_claimed()
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        if self.state.is_claimed() {
            return Err(PyRuntimeError::new_err("Cannot close a running event loop"));
        }
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        // R-122 "close with pending ops": tear down every live connection
        // and listener (cancels in-flight kernel ops), then drop all
        // pending work outside the cell.
        let pending = self.state.with(|st| {
            let tids: Vec<u64> = st.net.transports.keys().copied().collect();
            for tid in tids {
                net::teardown(&mut st.net, st.reactor.backend_mut(), tid, None);
            }
            let lids: Vec<u64> = st.net.listeners.keys().copied().collect();
            for lid in lids {
                net::listener_teardown(&mut st.net, st.reactor.backend_mut(), lid);
            }
            let mut dropped: Vec<Py<PyAny>> = st.reactor.clear_pending();
            dropped.extend(std::mem::take(&mut st.net.ready_scratch));
            for (_, h) in st.net.readers.drain() {
                dropped.push(h);
            }
            for (_, h) in st.net.writers.drain() {
                dropped.push(h);
            }
            st.net.events.clear(); // events hold only Py refs; move them too
            dropped
        })?;
        drop(pending);
        self.drain_graveyards(py)?;
        Ok(())
    }

    // ---- time (R-061) ----------------------------------------------------

    fn time(&self) -> PyResult<f64> {
        Ok(ticks_to_secs_f64(self.now_ns()?))
    }

    // ---- scheduling ------------------------------------------------------

    #[pyo3(signature = (callback, *args, context=None))]
    fn call_soon(
        &self,
        py: Python<'_>,
        callback: Bound<'_, PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<Handle>> {
        self.check_closed()?;
        let handle = self.make_handle(py, &callback, &args, context.as_ref(), "call_soon")?;
        let handle = Py::new(py, handle)?;
        let token: Py<PyAny> = handle.clone_ref(py).into_any();
        self.state.with(move |st| st.reactor.push_ready(token))?;
        Ok(handle)
    }

    /// Thread-safe variant (R-022): lock-free queue push + single wakeup.
    #[pyo3(signature = (callback, *args, context=None))]
    fn call_soon_threadsafe(
        &self,
        py: Python<'_>,
        callback: Bound<'_, PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<Handle>> {
        self.check_closed()?;
        let handle = self.make_handle(py, &callback, &args, context.as_ref(), "call_soon_threadsafe")?;
        let handle = Py::new(py, handle)?;
        if self.xqueue.push(handle.clone_ref(py).into_any()) {
            self.waker.wake();
        }
        Ok(handle)
    }

    #[pyo3(signature = (delay, callback, *args, context=None))]
    fn call_later(
        &self,
        py: Python<'_>,
        delay: f64,
        callback: Bound<'_, PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<TimerHandle>> {
        let now = self.now_ns()?;
        let deadline = if delay <= 0.0 { now } else { now.saturating_add(secs_f64_to_ticks(delay)) };
        self.schedule_timer(py, deadline, callback, args, context)
    }

    #[pyo3(signature = (when, callback, *args, context=None))]
    fn call_at(
        &self,
        py: Python<'_>,
        when: f64,
        callback: Bound<'_, PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<TimerHandle>> {
        self.schedule_timer(py, secs_f64_to_ticks(when), callback, args, context)
    }

    // ---- TCP (M1) --------------------------------------------------------

    /// Bind + listen (+ start the accept pool unless `start=false`).
    /// Returns (listener id, (ip, port) bound name, raw fd).
    #[pyo3(signature = (ip, port, factory, backlog=1024, reuse_addr=true, reuse_port=false, accept_pool=64, start=true))]
    #[allow(clippy::too_many_arguments)]
    fn tcp_listen(
        &self,
        py: Python<'_>,
        ip: &str,
        port: u16,
        factory: Bound<'_, PyAny>,
        backlog: i32,
        reuse_addr: bool,
        reuse_port: bool,
        accept_pool: usize,
        start: bool,
    ) -> PyResult<(u64, Py<PyAny>, u64)> {
        self.check_closed()?;
        let addr: std::net::IpAddr =
            ip.parse().map_err(|_| PyValueError::new_err(format!("invalid IP address: {ip:?}")))?;
        let sockaddr = std::net::SocketAddr::new(addr, port);
        let family = if sockaddr.is_ipv4() { netsys::AF_INET } else { netsys::AF_INET6 };
        let sock = netsys::create_tcp(family)?;
        let setup = (|| -> std::io::Result<()> {
            if reuse_addr {
                netsys::set_reuse_addr(sock, true)?;
            }
            if reuse_port {
                netsys::set_reuse_port(sock, true)?;
            }
            netsys::bind(sock, &netsys::build_sockaddr(sockaddr))?;
            netsys::listen(sock, backlog)
        })();
        if let Err(e) = setup {
            netsys::close(sock);
            return Err(e.into());
        }
        self.listen_socket(py, sock, factory, accept_pool, start)
    }

    /// Adopt an existing listening socket fd (create_server(sock=...)).
    #[pyo3(signature = (fd, factory, accept_pool=64, start=true))]
    fn listen_fd(
        &self,
        py: Python<'_>,
        fd: u64,
        factory: Bound<'_, PyAny>,
        accept_pool: usize,
        start: bool,
    ) -> PyResult<(u64, Py<PyAny>, u64)> {
        self.check_closed()?;
        self.listen_socket(py, fd as RawSocket, factory, accept_pool, start)
    }

    /// Start (or restart) accepting on a listener created with start=false.
    fn listener_start(&self, lid: u64) -> PyResult<()> {
        self.with_net(|net, reactor| net::listener_start(net, reactor.backend_mut(), lid))
    }

    fn listener_close(&self, py: Python<'_>, lid: u64) -> PyResult<()> {
        self.with_net(|net, reactor| net::listener_teardown(net, reactor.backend_mut(), lid))?;
        self.drain_graveyards(py)
    }

    /// Count of live connections spawned by any listener (facade
    /// wait_closed support is in Python; this is for stats).
    fn listener_count(&self) -> PyResult<usize> {
        self.with_net(|net, _| net.listeners.len())
    }

    /// Begin an async connect; `fut` resolves to the connected socket
    /// handle (u64) or an OSError.
    #[pyo3(signature = (ip, port, fut, local_ip=None, local_port=0))]
    fn tcp_connect(
        &self,
        _py: Python<'_>,
        ip: &str,
        port: u16,
        fut: Bound<'_, PyAny>,
        local_ip: Option<&str>,
        local_port: u16,
    ) -> PyResult<()> {
        self.check_closed()?;
        let addr: std::net::IpAddr =
            ip.parse().map_err(|_| PyValueError::new_err(format!("invalid IP address: {ip:?}")))?;
        let sockaddr = std::net::SocketAddr::new(addr, port);
        let family = if sockaddr.is_ipv4() { netsys::AF_INET } else { netsys::AF_INET6 };
        let sock = netsys::create_tcp(family)?;
        if let Some(lip) = local_ip {
            let laddr: std::net::IpAddr =
                lip.parse().map_err(|_| PyValueError::new_err(format!("invalid local IP: {lip:?}")))?;
            let lsa = netsys::build_sockaddr(std::net::SocketAddr::new(laddr, local_port));
            if let Err(e) = netsys::bind(sock, &lsa) {
                netsys::close(sock);
                return Err(e.into());
            }
        }
        let fut: Py<PyAny> = fut.unbind();
        let sa = netsys::build_sockaddr(sockaddr);
        let res = self.with_net(|net, reactor| -> std::io::Result<()> {
            let backend = reactor.backend_mut();
            backend.register_socket(sock)?;
            let op = backend.post_connect(sock, &sa.buf[..sa.len])?;
            net.ops.insert(op, net::OpTarget::Connect { fut, sock });
            Ok(())
        })?;
        if let Err(e) = res {
            netsys::close(sock);
            return Err(e.into());
        }
        Ok(())
    }

    /// Wire a connected socket (from tcp_connect's future) into a transport.
    fn attach_stream(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        sock: u64,
        protocol: Bound<'_, PyAny>,
    ) -> PyResult<Py<Transport>> {
        slf.get().check_closed()?;
        net::wire_stream(py, slf, sock as RawSocket, protocol)
    }

    // ---- readiness watches (R-057) ---------------------------------------

    #[pyo3(signature = (fd, callback, *args))]
    fn add_reader(
        &self,
        py: Python<'_>,
        fd: u64,
        callback: Bound<'_, PyAny>,
        args: Bound<'_, PyTuple>,
    ) -> PyResult<Py<Handle>> {
        self.check_closed()?;
        let handle = self.make_handle(py, &callback, &args, None, "add_reader")?;
        let handle = Py::new(py, handle)?;
        let token: Py<PyAny> = handle.clone_ref(py).into_any();
        let sock = fd as RawSocket;
        self.with_net(|net, reactor| -> std::io::Result<()> {
            if let Some(old) = net.readers.insert(sock, token) {
                net.graveyard_py.push(old);
            }
            let w = net.writers.contains_key(&sock);
            reactor.backend_mut().set_watch(sock, true, w)
        })??;
        self.drain_graveyards(py)?;
        Ok(handle)
    }

    fn remove_reader(&self, py: Python<'_>, fd: u64) -> PyResult<bool> {
        let sock = fd as RawSocket;
        let removed = self.with_net(|net, reactor| -> std::io::Result<bool> {
            let removed = net.readers.remove(&sock);
            let had = removed.is_some();
            if let Some(h) = removed {
                net.graveyard_py.push(h);
            }
            let w = net.writers.contains_key(&sock);
            if had || w {
                reactor.backend_mut().set_watch(sock, false, w)?;
            }
            Ok(had)
        })??;
        self.drain_graveyards(py)?;
        Ok(removed)
    }

    #[pyo3(signature = (fd, callback, *args))]
    fn add_writer(
        &self,
        py: Python<'_>,
        fd: u64,
        callback: Bound<'_, PyAny>,
        args: Bound<'_, PyTuple>,
    ) -> PyResult<Py<Handle>> {
        self.check_closed()?;
        let handle = self.make_handle(py, &callback, &args, None, "add_writer")?;
        let handle = Py::new(py, handle)?;
        let token: Py<PyAny> = handle.clone_ref(py).into_any();
        let sock = fd as RawSocket;
        self.with_net(|net, reactor| -> std::io::Result<()> {
            if let Some(old) = net.writers.insert(sock, token) {
                net.graveyard_py.push(old);
            }
            let r = net.readers.contains_key(&sock);
            reactor.backend_mut().set_watch(sock, r, true)
        })??;
        self.drain_graveyards(py)?;
        Ok(handle)
    }

    fn remove_writer(&self, py: Python<'_>, fd: u64) -> PyResult<bool> {
        let sock = fd as RawSocket;
        let removed = self.with_net(|net, reactor| -> std::io::Result<bool> {
            let removed = net.writers.remove(&sock);
            let had = removed.is_some();
            if let Some(h) = removed {
                net.graveyard_py.push(h);
            }
            let r = net.readers.contains_key(&sock);
            if had || r {
                reactor.backend_mut().set_watch(sock, r, false)?;
            }
            Ok(had)
        })??;
        self.drain_graveyards(py)?;
        Ok(removed)
    }

    // ---- hooks / config ----------------------------------------------------

    fn set_error_hook(&self, hook: Bound<'_, PyAny>) {
        let _ = self.error_hook.set(hook.unbind());
    }

    fn set_net_error_hook(&self, hook: Bound<'_, PyAny>) {
        let _ = self.net_error_hook.set(hook.unbind());
    }

    fn set_slow_callback_hook(&self, hook: Bound<'_, PyAny>) {
        let _ = self.slow_callback_hook.set(hook.unbind());
    }

    fn set_debug(&self, enabled: bool) {
        self.debug.store(enabled, Ordering::Relaxed);
    }

    fn get_debug(&self) -> bool {
        self.debug.load(Ordering::Relaxed)
    }

    // ---- introspection (R-103) --------------------------------------------

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let (stats, ready, timers, netstats) = self.state.with(|st| {
            (
                st.reactor.stats.clone(),
                st.reactor.ready_len(),
                st.reactor.timers_len(),
                (
                    st.net.transports.len(),
                    st.net.listeners.len(),
                    st.net.buffers.in_use(),
                    st.net.stats_bytes_rx,
                    st.net.stats_bytes_tx,
                    st.net.stats_conns_accepted,
                ),
            )
        })?;
        let d = PyDict::new(py);
        d.set_item("backend", self.backend_name)?;
        d.set_item("ticks", stats.ticks)?;
        d.set_item("polls", stats.polls)?;
        d.set_item("completions", stats.completions)?;
        d.set_item("callbacks_dispatched", stats.callbacks_dispatched)?;
        d.set_item("timers_fired", stats.timers_fired)?;
        d.set_item("xthread_items", stats.xthread_items)?;
        d.set_item("spin_hits", stats.spin_hits)?;
        d.set_item("ready_len", ready)?;
        d.set_item("timers_len", timers)?;
        d.set_item("connections", netstats.0)?;
        d.set_item("listeners", netstats.1)?;
        d.set_item("buffers_in_use", netstats.2)?;
        d.set_item("bytes_received", netstats.3)?;
        d.set_item("bytes_sent", netstats.4)?;
        d.set_item("connections_accepted", netstats.5)?;
        Ok(d)
    }
}

impl CoreLoop {
    fn schedule_timer(
        &self,
        py: Python<'_>,
        deadline_ns: Ticks,
        callback: Bound<'_, PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<TimerHandle>> {
        self.check_closed()?;
        let base = self.make_handle(py, &callback, &args, context.as_ref(), "call_later")?;
        let token = TimerToken::new();
        let timer = TimerHandle {
            token: token.clone(),
            when_ns: deadline_ns,
            cancel_counter: self.timer_cancels.clone(),
        };
        let handle = Py::new(py, PyClassInitializer::from(base).add_subclass(timer))?;
        let payload: Py<PyAny> = handle.clone_ref(py).into_any();
        self.state.with(move |st| st.reactor.schedule_timer_with_token(deadline_ns, payload, token))?;
        Ok(handle)
    }

    fn listen_socket(
        &self,
        py: Python<'_>,
        sock: RawSocket,
        factory: Bound<'_, PyAny>,
        accept_pool: usize,
        start: bool,
    ) -> PyResult<(u64, Py<PyAny>, u64)> {
        let sockname = netsys::sockname(sock).ok();
        let name_obj = sockname
            .map(|a| {
                let t = (a.ip().to_string(), a.port());
                t.into_pyobject(py).map(|b| b.into_any().unbind())
            })
            .transpose()?
            .unwrap_or_else(|| py.None());
        let factory: Py<PyAny> = factory.unbind();
        let lid = self.with_net(|net, reactor| -> std::io::Result<u64> {
            reactor.backend_mut().register_socket(sock)?;
            let lid = net::listener_create(net, sock, factory, accept_pool);
            if start {
                net::listener_start(net, reactor.backend_mut(), lid);
            }
            Ok(lid)
        })??;
        Ok((lid, name_obj, sock as u64))
    }
}
