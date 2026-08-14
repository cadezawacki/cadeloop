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
use cadeloop_core::http::Limits;
use cadeloop_core::netsys;
use cadeloop_core::reactor::{Reactor, ReactorConfig};
use cadeloop_core::ready::CrossThreadQueue;
use cadeloop_core::time::{secs_f64_to_ticks, ticks_to_secs_f64, Ticks};
use cadeloop_core::timer::TimerToken;
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError, PySystemExit, PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use pyo3::{PyTraverseError, PyVisit};

use crate::gil_boundary::StateCell;
use crate::handles::{run_handle, DispatchOutcome, Handle, TimerHandle};
use crate::net::{self, NetEvent, NetState, Transport};
use crate::taskfast;

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
    /// Consecutive idle ticks, for the ADR-24 idle-state trace only.
    idle_ticks: AtomicU64,
    /// Facade hook: `(handle, exception) -> None` for callback errors.
    error_hook: OnceLock<Py<PyAny>>,
    /// Facade hook: `(message, exception) -> None` for protocol/net errors.
    net_error_hook: OnceLock<Py<PyAny>>,
    /// Facade hook: `(handle, seconds) -> None` (R-142 slow callbacks).
    slow_callback_hook: OnceLock<Py<PyAny>>,
    /// The `cadeloop.Loop` facade that owns this core, so the native
    /// `create_task` / `create_future` fast paths can pass `loop=` without
    /// a round trip through Python. Strong, like the hooks above -- the
    /// facade and its core have the same lifetime.
    owner: OnceLock<Py<PyAny>>,
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

    fn require_owner(&self) -> PyResult<&Py<PyAny>> {
        self.owner.get().ok_or_else(|| {
            PyRuntimeError::new_err("cadeloop: core has no owning Loop (set_owner was never called)")
        })
    }

    /// Hand a `create_task` call the native path does not model back to the
    /// facade's own method. Looked up on the *type*, never the instance, so
    /// this cannot re-enter the native binding the facade installed on the
    /// instance -- and so a `Loop` subclass that overrides `create_task`
    /// still gets its override (the facade only installs the fast path when
    /// the method is unoverridden, which makes the type lookup exact).
    fn create_task_via_facade(
        &self,
        py: Python<'_>,
        owner: &Py<PyAny>,
        coro: Bound<'_, PyAny>,
        name: Option<Bound<'_, PyAny>>,
        context: Option<Bound<'_, PyAny>>,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        let owner = owner.bind(py);
        let slow = owner.get_type().getattr(intern!(py, "create_task"))?;
        let kw = PyDict::new(py);
        if let Some(extra) = kwargs {
            for (k, v) in extra.iter() {
                kw.set_item(k, v)?;
            }
        }
        if let Some(n) = name {
            kw.set_item(intern!(py, "name"), n)?;
        }
        if let Some(c) = context {
            kw.set_item(intern!(py, "context"), c)?;
        }
        slow.call((owner, coro), Some(&kw)).map(Bound::unbind)
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
        let (entries, bufs, pys, protos, timers) = self.state.with(|st| {
            (
                std::mem::take(&mut st.net.graveyard_entries),
                std::mem::take(&mut st.net.graveyard_bufs),
                std::mem::take(&mut st.net.graveyard_py),
                std::mem::take(&mut st.net.graveyard_protos),
                st.reactor.take_graveyard(),
            )
        })?;
        drop((entries, bufs, pys, protos, timers));
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
        // Debug-gated, exactly as CPython does it: base_events.call_soon
        // runs _check_callback (its callable + coroutine-function test)
        // only under `if self._debug`. Running it unconditionally made
        // cadeloop STRICTER than the stdlib it mirrors, and cost two
        // PyObject_GetAttr round-trips (__code__, co_flags) on every
        // call_soon / call_later / call_at / add_reader / add_writer --
        // the busiest path in the loop. uvloop, rloop and rsloop skip the
        // check entirely; matching the stdlib's own gating keeps the clear
        // TypeError where a developer will actually see it.
        if self.debug.load(Ordering::Relaxed) {
            if !callback.is_callable() {
                return Err(PyTypeError::new_err(format!(
                    "a callable object was expected by {method}(), got {}",
                    callback.repr()?
                )));
            }
            if is_coroutine_function(py, callback) {
                return Err(PyTypeError::new_err(format!("coroutines cannot be used with {method}()")));
            }
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
    ///
    /// A pure-scheduling tick (no net events — the call_soon-chain shape)
    /// enters the state cell exactly ONCE: flush + prepare + poll +
    /// translate + batch-take all under a single claim. rloop's tick
    /// anatomy showed the per-tick claim/release rounds are what a
    /// queue-depth-1 chain actually benchmarks.
    fn tick(&self, slf: &Bound<'_, CoreLoop>, py: Python<'_>) -> PyResult<()> {
        // TEMPORARY (ADR-24): bisecting a Windows-only worker-model crash.
        // Prior tracing (server.py, _winworker.py) proved every setup stage
        // through "about to call run_forever" succeeds; further tracing
        // directly in iocp.rs proved take_accept_socket is never reached
        // (no accept ever completes before the crash) — so the crash is in
        // this tick's poll/translate machinery itself, on the very first
        // call. Env-gated (CADELOOP_TRACE_TICK) since this is a genuine
        // hot path exercised by every test, not just the worker model.
        let trace_tick = trace_tick_enabled();
        let stopping = self.stopping.load(Ordering::Acquire);
        DISPATCH_BUF.with_borrow_mut(|batch| -> PyResult<()> {
            debug_assert!(batch.is_empty());
            type TickOut = (std::io::Result<()>, bool, Vec<NetEvent>, Graveyards);
            let (poll_result, parked, events, graveyard): TickOut = self.state.with(|st| {
                // Phase 1: flush corked writes from last tick's callbacks.
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
                let (poll_result, parked) = if timeout.is_zero() {
                    // Non-blocking reap: keeping the GIL is legal (nothing
                    // can block) and skips a save/restore of thread state.
                    (reactor.poll(timeout), false)
                } else {
                    // R-021: the only GIL release point; sound because
                    // `claim` guarantees no other thread enters this state.
                    (py.detach(move || reactor.poll(timeout)), true)
                };
                if poll_result.is_err() {
                    return (
                        poll_result,
                        parked,
                        Vec::new(),
                        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
                    );
                }
                // Phase 2: translate completions (no Python execution).
                st.reactor.finish_poll_after(parked);
                if parked {
                    self.cached_time_ns.store(st.reactor.time_cached(), Ordering::Release);
                }
                let mut comps = std::mem::take(&mut st.completions_scratch);
                st.reactor.drain_completions(&mut comps);
                // ADR-24: a per-tick log of "drained 0 completions" told us
                // only that the loop was idle, which we already knew. What
                // the /bg hang needs is the STATE it is idle in: whether an
                // accept is outstanding, whether the connection exists,
                // whether anything is queued. Printed only while idle and
                // only every IDLE_TRACE_EVERY ticks, so a healthy run pays
                // nothing and a hung one produces a readable trace instead
                // of thousands of identical lines.
                if trace_tick && parked && comps.is_empty() {
                    let n = self.idle_ticks.fetch_add(1, Ordering::Relaxed) + 1;
                    if n.is_multiple_of(IDLE_TRACE_EVERY) {
                        let (accept_ops, listeners): (usize, usize) = (
                            st.net.listeners.values().map(|l| l.accept_ops_len()).sum(),
                            st.net.listeners.len(),
                        );
                        let foreign = st.reactor.backend_mut().foreign_completions();
                        eprintln!(
                            "cadeloop-tick: idle x{n} listeners={listeners} accept_ops={accept_ops} \
                             transports={} ops={} [{}] reap_guards={} events={} ready={} timers={} \
                             starved={} foreign_completions={foreign}{}",
                            st.net.transports.len(),
                            st.net.ops.len(),
                            net::op_target_breakdown(&st.net),
                            st.net.reap_guards.len(),
                            st.net.events.len(),
                            st.reactor.ready_len(),
                            st.reactor.timers_len(),
                            st.net.any_starved_listener,
                            net::transport_breakdown(&st.net),
                        );
                        let _ = std::io::Write::flush(&mut std::io::stderr());
                    }
                } else if trace_tick {
                    self.idle_ticks.store(0, Ordering::Relaxed);
                }
                if !comps.is_empty() {
                    net::translate(py, &mut st.net, st.reactor.backend_mut(), &comps);
                    // A listener whose accept pool ran dry on a transient
                    // failure has no completion left to retry it (R-032).
                    net::retry_starved_listeners(&mut st.net, st.reactor.backend_mut());
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
                let events = std::mem::take(&mut st.net.events);
                if events.is_empty() {
                    // Pure-scheduling tick: take the dispatch batch in the
                    // SAME cell entry (batch_left was snapshotted by
                    // prepare_tick, so semantics match a later take).
                    while let Some(token) = st.reactor.pop_ready_batched() {
                        batch.push(token);
                    }
                }
                (Ok(()), parked, events, take_graveyards(st))
            })?;
            drop(graveyard);
            poll_result?;

            // Ctrl-C responsiveness: a parked poll returns promptly on
            // EINTR, so check right after it; busy ticks check every 64th
            // tick instead of paying the call per tick. The batch may
            // already be filled — hand tokens back (front, reverse order)
            // so an interrupt loses nothing and preserves FIFO.
            let tick_no = self.tick_no.fetch_add(1, Ordering::Relaxed);
            if parked || tick_no & 63 == 0 {
                let interrupted = unsafe { ffi::PyErr_CheckSignals() != 0 };
                if interrupted {
                    let err = PyErr::fetch(py);
                    if !batch.is_empty() {
                        self.state.with(|st| {
                            for token in batch.drain(..).rev() {
                                st.reactor.unpop_ready(token);
                            }
                        })?;
                    }
                    return Err(err);
                }
            }

            // Phases 3-5 only exist when network events fired: dispatch
            // protocol callbacks, flush the writes they corked (same tick,
            // R-035), then dispatch teardowns produced by that flush.
            if !events.is_empty() {
                net::dispatch_events(py, slf, events)?;
                let events = self.flush_corked(py)?;
                self.drain_graveyards(py)?;
                if !events.is_empty() {
                    net::dispatch_events(py, slf, events)?;
                }
                // The batch was not taken in-cell on this path.
                self.state.with(|st| {
                    while let Some(token) = st.reactor.pop_ready_batched() {
                        batch.push(token);
                    }
                })?;
            }

            // Phase 6: ready-callback batch (R-054); the buffer lives in
            // loop-thread TLS and is always emptied before being parked —
            // a fatal error (KeyboardInterrupt/SystemExit from a callback)
            // returns the undispatched tail to the queue front so nothing
            // is lost and FIFO order holds across the unwind.
            let debug = self.debug.load(Ordering::Relaxed);
            let mut fatal: Option<(usize, PyErr)> = None;
            for (idx, token) in batch.iter().enumerate() {
                let started = debug.then(std::time::Instant::now);
                match run_handle(py, token.bind(py)) {
                    Ok(DispatchOutcome::Done) => {}
                    Ok(DispatchOutcome::Failed(err)) => self.report_failure(py, token, err),
                    Err(e) => {
                        fatal = Some((idx, e));
                        break;
                    }
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
            if let Some((idx, err)) = fatal {
                // idx ran (and raised); everything after it goes back.
                if idx + 1 < batch.len() {
                    self.state.with(|st| {
                        for token in batch.drain(idx + 1..).rev() {
                            st.reactor.unpop_ready(token);
                        }
                    })?;
                }
                batch.clear();
                return Err(err);
            }
            batch.clear();
            Ok(())
        })
    }
}

thread_local! {
    /// Reusable dispatch buffer (loop thread only; empty between ticks).
    static DISPATCH_BUF: std::cell::RefCell<Vec<Py<PyAny>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

type Graveyards = (
    Vec<crate::net::TransportEntry>,
    Vec<crate::net::WriteBuf>,
    Vec<Py<PyAny>>,
    Vec<crate::net::ProtoKind>,
    Vec<Py<PyAny>>,
);

fn take_graveyards(st: &mut LoopState) -> Graveyards {
    (
        std::mem::take(&mut st.net.graveyard_entries),
        std::mem::take(&mut st.net.graveyard_bufs),
        std::mem::take(&mut st.net.graveyard_py),
        std::mem::take(&mut st.net.graveyard_protos),
        st.reactor.take_graveyard(),
    )
}

/// CO_COROUTINE (stable since 3.5; this project pins CPython 3.11).
/// Matches inspect.iscoroutinefunction's core check for `async def`
/// functions and bound methods (both expose `__code__` transparently)
/// without a Python-level call — call_soon/call_soon_threadsafe run
/// this on every scheduled callback (R-050 hot path), so the cheaper
/// attribute-chase beats round-tripping through asyncio.iscoroutine*.
/// Doesn't unwrap functools.partial/other wrappers like the real
/// asyncio.iscoroutinefunction does — the common mistake (passing an
/// `async def` function or bound method directly) is what this catches.
const CO_COROUTINE: i32 = 0x80;

/// TEMPORARY (ADR-24): CADELOOP_TRACE_TICK-gated tick tracing for the
/// Windows worker-model crash bisection. Checked once per process via a
/// cached OnceLock (a plain per-tick env var read would itself be a
/// measurable hot-path cost). Remove alongside the eprintln! call sites
/// in `CoreLoop::tick` once the crash site is found.
/// How many consecutive idle ticks between ADR-24 state dumps. At the
/// 100ms park this is roughly one line every two seconds.
const IDLE_TRACE_EVERY: u64 = 20;

fn trace_tick_enabled() -> bool {
    static TRACE_TICK: OnceLock<bool> = OnceLock::new();
    *TRACE_TICK.get_or_init(|| std::env::var_os("CADELOOP_TRACE_TICK").is_some())
}

fn is_coroutine_function(py: Python<'_>, callback: &Bound<'_, PyAny>) -> bool {
    let Ok(code) = callback.getattr(intern!(py, "__code__")) else { return false };
    let Ok(flags) = code.getattr(intern!(py, "co_flags")) else { return false };
    flags.extract::<i32>().map(|f| f & CO_COROUTINE != 0).unwrap_or(false)
}

pub(crate) fn copy_context(py: Python<'_>) -> PyResult<Py<PyAny>> {
    unsafe {
        let ptr = ffi::PyContext_CopyCurrent();
        if ptr.is_null() {
            return Err(PyErr::fetch(py));
        }
        Ok(Bound::from_owned_ptr(py, ptr).unbind())
    }
}

#[pymethods]
impl CoreLoop {
    #[new]
    #[pyo3(signature = (backend="auto", spin_us=20, high_water=65536, low_water=16384,
                        rio_cq_size=65536, rio_rq_recv=32, rio_rq_send=32))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        backend: &str,
        spin_us: u64,
        high_water: usize,
        low_water: usize,
        rio_cq_size: u32,
        rio_rq_recv: u32,
        rio_rq_send: u32,
    ) -> PyResult<Self> {
        let kind = BackendKind::parse(backend).ok_or_else(|| {
            PyValueError::new_err(format!(
                "invalid backend {backend:?}: expected 'auto', 'iocp', 'rio' or 'epoll'"
            ))
        })?;
        let cfg = ReactorConfig {
            backend: kind,
            spin_us,
            backend_opts: cadeloop_core::backend::BackendOptions { rio_cq_size, rio_rq_recv, rio_rq_send },
            ..Default::default()
        };
        let reactor: Reactor<Py<PyAny>> = Reactor::new(cfg)?;
        let (xqueue, waker) = reactor.cross_thread_handles();
        let timer_cancels = reactor.timer_cancel_counter();
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
            idle_ticks: AtomicU64::new(0),
            error_hook: OnceLock::new(),
            net_error_hook: OnceLock::new(),
            slow_callback_hook: OnceLock::new(),
            owner: OnceLock::new(),
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
        let (pending, events) = self.state.with(|st| {
            let tids: Vec<u64> = st.net.transports.keys().copied().collect();
            for tid in tids {
                net::teardown(&mut st.net, st.reactor.backend_mut(), tid, None);
            }
            let lids: Vec<u64> = st.net.listeners.keys().copied().collect();
            for lid in lids {
                net::listener_teardown(&mut st.net, st.reactor.backend_mut(), lid);
            }
            // R-058 datagram endpoints: cancel ops, close sockets, release
            // slots; Python refs drop outside the cell with the rest.
            let dids: Vec<u64> = st.net.datagrams.keys().copied().collect();
            for did in dids {
                net::udp_teardown_at_close(&mut st.net, st.reactor.backend_mut(), did);
            }
            // R-051/R-122: ops owned by no transport, listener or
            // datagram endpoint (a pending connect, a Windows pipe
            // read/write) are not reached by any of the three sweeps
            // above, so they would keep their futures, buffers and (for
            // connect) an open socket alive for the closed core's
            // lifetime.
            let mut dropped: Vec<Py<PyAny>> =
                net::cancel_standalone_ops(&mut st.net, st.reactor.backend_mut());
            dropped.extend(st.reactor.clear_pending());
            dropped.extend(std::mem::take(&mut st.net.ready_scratch));
            for (_, h) in st.net.readers.drain() {
                dropped.push(h);
            }
            for (_, h) in st.net.writers.drain() {
                dropped.push(h);
            }
            // ADR-5: NetEvent variants OWN Python references. Clearing the
            // vector here would decref them while `with` holds the exclusive
            // &mut, and a finalizer reached by one of those decrefs can
            // re-enter the loop and alias the state. Move them out and drop
            // after leaving the cell, exactly as the tick path does.
            (dropped, std::mem::take(&mut st.net.events))
        })?;
        drop(pending);
        drop(events);
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
        let sock = bind_listen_socket(ip, port, backlog, reuse_addr, reuse_port)?;
        self.listen_socket(py, sock, net::ListenerKind::Factory(factory.unbind()), accept_pool, start)
    }

    /// Bind + listen for the native HTTP/ASGI engine (M2, R-080). Every
    /// accepted connection is parsed and answered natively; `pyloop` is
    /// the facade loop (receive() waiters / non-eager tasks need it).
    #[pyo3(signature = (ip, port, app, pyloop, state=None, backlog=1024, reuse_addr=true,
                        reuse_port=false, accept_pool=64, eager=true, max_header_bytes=65536,
                        max_headers=100, max_url=8192, max_body=None,
                        request_line_timeout=5.0, keepalive_idle=75.0, tls=None))]
    #[allow(clippy::too_many_arguments)]
    fn http_listen(
        &self,
        py: Python<'_>,
        ip: &str,
        port: u16,
        app: Bound<'_, PyAny>,
        pyloop: Bound<'_, PyAny>,
        state: Option<Bound<'_, PyAny>>,
        backlog: i32,
        reuse_addr: bool,
        reuse_port: bool,
        accept_pool: usize,
        eager: bool,
        max_header_bytes: usize,
        max_headers: usize,
        max_url: usize,
        max_body: Option<usize>,
        request_line_timeout: f64,
        keepalive_idle: f64,
        tls: Option<Bound<'_, PyAny>>,
    ) -> PyResult<(u64, Py<PyAny>, u64)> {
        self.check_closed()?;
        if !app.is_callable() {
            return Err(PyTypeError::new_err("ASGI app must be callable"));
        }
        let state: Py<PyAny> = match state {
            Some(s) if !s.is_none() => s.unbind(),
            _ => PyDict::new(py).into_any().unbind(),
        };
        let secs_to_ns = |s: f64| if s > 0.0 { (s * 1e9) as u64 } else { 0 };
        let sock = bind_listen_socket(ip, port, backlog, reuse_addr, reuse_port)?;
        let kind = net::ListenerKind::Http {
            app: app.unbind(),
            pyloop: pyloop.unbind(),
            state,
            limits: Limits { max_header_bytes, max_headers, max_url, max_body },
            eager,
            tuning: net::HttpTuning {
                head_timeout_ns: secs_to_ns(request_line_timeout),
                idle_timeout_ns: secs_to_ns(keepalive_idle),
            },
            tls: tls.filter(|t| !t.is_none()).map(|t| t.unbind()),
        };
        self.listen_socket(py, sock, kind, accept_pool, true)
    }

    /// Adopt an EXISTING listening socket for the native HTTP engine —
    /// the Windows worker model hands each spawned worker a
    /// WSADuplicateSocketW-shared listener (R-090); also usable anywhere
    /// a pre-bound socket exists. The engine owns the socket from here.
    #[pyo3(signature = (fd, app, pyloop, state=None, accept_pool=64, eager=true,
                        max_header_bytes=65536, max_headers=100, max_url=8192, max_body=None,
                        request_line_timeout=5.0, keepalive_idle=75.0, tls=None))]
    #[allow(clippy::too_many_arguments)]
    fn http_listen_fd(
        &self,
        py: Python<'_>,
        fd: u64,
        app: Bound<'_, PyAny>,
        pyloop: Bound<'_, PyAny>,
        state: Option<Bound<'_, PyAny>>,
        accept_pool: usize,
        eager: bool,
        max_header_bytes: usize,
        max_headers: usize,
        max_url: usize,
        max_body: Option<usize>,
        request_line_timeout: f64,
        keepalive_idle: f64,
        tls: Option<Bound<'_, PyAny>>,
    ) -> PyResult<(u64, Py<PyAny>, u64)> {
        self.check_closed()?;
        if !app.is_callable() {
            return Err(PyTypeError::new_err("ASGI app must be callable"));
        }
        let state: Py<PyAny> = match state {
            Some(s) if !s.is_none() => s.unbind(),
            _ => PyDict::new(py).into_any().unbind(),
        };
        let secs_to_ns = |s: f64| if s > 0.0 { (s * 1e9) as u64 } else { 0 };
        let kind = net::ListenerKind::Http {
            app: app.unbind(),
            pyloop: pyloop.unbind(),
            state,
            limits: Limits { max_header_bytes, max_headers, max_url, max_body },
            eager,
            tuning: net::HttpTuning {
                head_timeout_ns: secs_to_ns(request_line_timeout),
                idle_timeout_ns: secs_to_ns(keepalive_idle),
            },
            tls: tls.filter(|t| !t.is_none()).map(|t| t.unbind()),
        };
        self.listen_socket(py, fd as RawSocket, kind, accept_pool, true)
    }

    /// Adopt an already-ACCEPTED, connected socket as an HTTP connection
    /// (R-090 Windows worker model). The supervisor owns the accept loop
    /// and hands each connection over a process boundary; the receiving
    /// worker associates it with ITS OWN completion port here.
    ///
    /// The socket must not have been associated with any completion port
    /// before it arrives (ADR-25: a file object binds to exactly one port
    /// for life). That holds because the supervisor accepts with a plain
    /// blocking `accept()` on a listener it never registers with an IOCP.
    #[pyo3(signature = (fd, app, pyloop, state=None, eager=true,
                        max_header_bytes=65536, max_headers=100, max_url=8192, max_body=None,
                        request_line_timeout=5.0, keepalive_idle=75.0, tls=None))]
    #[allow(clippy::too_many_arguments)]
    fn http_adopt(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        fd: u64,
        app: Bound<'_, PyAny>,
        pyloop: Bound<'_, PyAny>,
        state: Option<Bound<'_, PyAny>>,
        eager: bool,
        max_header_bytes: usize,
        max_headers: usize,
        max_url: usize,
        max_body: Option<usize>,
        request_line_timeout: f64,
        keepalive_idle: f64,
        tls: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        slf.get().check_closed()?;
        if !app.is_callable() {
            return Err(PyTypeError::new_err("ASGI app must be callable"));
        }
        let state: Py<PyAny> = match state {
            Some(s) if !s.is_none() => s.unbind(),
            _ => PyDict::new(py).into_any().unbind(),
        };
        let secs_to_ns = |s: f64| if s > 0.0 { (s * 1e9) as u64 } else { 0 };
        net::wire_http(
            py,
            slf,
            fd as RawSocket,
            app.unbind(),
            pyloop.unbind(),
            state,
            Limits { max_header_bytes, max_headers, max_url, max_body },
            eager,
            net::HttpTuning {
                head_timeout_ns: secs_to_ns(request_line_timeout),
                idle_timeout_ns: secs_to_ns(keepalive_idle),
            },
            tls.filter(|t| !t.is_none()).map(|t| t.unbind()),
        )
    }

    /// R-058: adopt a bound (optionally connected) UDP socket as a
    /// datagram endpoint. Callbacks are the protocol's datagram_received /
    /// error_received / connection_lost bound methods. Returns the did.
    fn udp_open(
        &self,
        fd: u64,
        datagram_received: Bound<'_, PyAny>,
        error_received: Bound<'_, PyAny>,
        connection_lost: Bound<'_, PyAny>,
    ) -> PyResult<u64> {
        self.check_closed()?;
        let did = self.with_net(|net, reactor| {
            net::udp_wire(
                net,
                reactor.backend_mut(),
                fd as cadeloop_core::backend::RawSocket,
                datagram_received.clone().unbind(),
                error_received.clone().unbind(),
                connection_lost.clone().unbind(),
            )
        })??;
        Ok(did)
    }

    /// R-058: swap a datagram endpoint's cached protocol callbacks so
    /// `DatagramTransport.set_protocol()` actually redirects events.
    fn udp_set_callbacks(
        &self,
        py: Python<'_>,
        did: u64,
        datagram_received: Bound<'_, PyAny>,
        error_received: Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let swapped = self.with_net(|net, _| {
            net::udp_set_callbacks(net, did, datagram_received.unbind(), error_received.unbind())
        })?;
        self.drain_graveyards(py)?;
        Ok(swapped)
    }

    /// Bytes queued behind a datagram endpoint's in-flight send (R-058).
    fn udp_queued_bytes(&self, did: u64) -> PyResult<usize> {
        self.with_net(|net, _| net::udp_queued_bytes(net, did))
    }

    /// R-058 sendto. `addr` None = connected-mode send().
    #[pyo3(signature = (did, data, addr=None))]
    fn udp_sendto(&self, py: Python<'_>, did: u64, data: &[u8], addr: Option<(String, u16)>) -> PyResult<()> {
        self.check_closed()?;
        let addr = match addr {
            Some((ip, port)) => {
                let ip: std::net::IpAddr =
                    ip.parse().map_err(|_| PyValueError::new_err(format!("invalid IP address: {ip:?}")))?;
                Some(std::net::SocketAddr::new(ip, port))
            }
            None => None,
        };
        self.with_net(|net, reactor| net::udp_sendto(py, net, reactor.backend_mut(), did, data, addr))??;
        self.drain_graveyards(py)
    }

    /// R-058 close/abort. close flushes queued sends; abort drops them.
    #[pyo3(signature = (did, abort=false))]
    fn udp_close(&self, py: Python<'_>, did: u64, abort: bool) -> PyResult<()> {
        self.with_net(|net, reactor| {
            net::udp_close(py, net, reactor.backend_mut(), did, abort);
        })?;
        self.drain_graveyards(py)
    }

    /// R-080 timeout sweep tick (armed as a coarse repeating timer by the
    /// facade server). Returns (head_timeouts, idle_closes) this pass.
    fn http_sweep(&self, py: Python<'_>) -> PyResult<(u32, u32)> {
        let counts = self.with_net(|net, reactor| {
            let now_ns = reactor.time_cached();
            let backend = reactor.backend_mut();
            net::http_sweep(py, net, backend, now_ns)
        })?;
        self.drain_graveyards(py)?;
        Ok(counts)
    }

    /// R-140: install (or clear) the access-log sink — a callable
    /// receiving (peername, method, target_bytes, status, duration_ms).
    #[pyo3(signature = (sink=None))]
    fn set_access_log(&self, sink: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        self.with_net(|net, _| {
            net.access_sink = sink.map(|s| s.unbind());
        })
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
        self.listen_socket(
            py,
            fd as RawSocket,
            net::ListenerKind::Factory(factory.unbind()),
            accept_pool,
            start,
        )
    }

    /// Start (or restart) accepting on a listener created with start=false.
    fn listener_start(&self, lid: u64) -> PyResult<()> {
        self.with_net(|net, reactor| net::listener_start(net, reactor.backend_mut(), lid))??;
        Ok(())
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
            match backend.post_connect(sock, &sa.buf[..sa.len]) {
                Ok(op) => {
                    net.ops.insert(op, net::OpTarget::Connect { fut, sock });
                    Ok(())
                }
                Err(e) => {
                    // register_socket succeeded, so this socket IS
                    // associated; closing it without detaching would leave
                    // a stale entry that a recycled handle inherits.
                    backend.detach_socket(sock);
                    Err(e)
                }
            }
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

    // ---- named pipes (R-051 Windows subprocess/pipe support) -------------
    // Mechanical primitives only: post an overlapped op, resolve `fut` on
    // completion. All Transport/Protocol semantics (EOF, backpressure,
    // pause/resume) live in Python, mirroring stdlib's own proactor pipe
    // transports (python/cadeloop/_winpipes.py) — pipes are not the hot
    // path, so there is no native-transport machinery here.

    /// Associate a pipe HANDLE with the backend's completion port.
    fn pipe_register(&self, handle: u64) -> PyResult<()> {
        self.check_closed()?;
        self.with_net(|_, reactor| reactor.backend_mut().register_pipe(handle as RawSocket))??;
        Ok(())
    }

    /// Begin an overlapped read; `fut` resolves to the bytes read (empty
    /// = EOF) or an OSError.
    fn pipe_read(&self, handle: u64, nbytes: usize, fut: Bound<'_, PyAny>) -> PyResult<()> {
        self.check_closed()?;
        let py = fut.py();
        let mut buf = vec![0u8; nbytes];
        let ptr = buf.as_mut_ptr();
        let fut: Py<PyAny> = fut.unbind();
        // Held for the ERROR_BROKEN_PIPE-on-post path below — `fut` itself
        // moves into OpTarget::PipeRead on the (overwhelmingly common)
        // success path, so it's gone by the time `?` could inspect it.
        let fut_for_broken_pipe = fut.clone_ref(py);
        let res = self.with_net(|net, reactor| -> std::io::Result<()> {
            let op = reactor.backend_mut().post_pipe_read(handle as RawSocket, ptr, nbytes as u32)?;
            net.ops.insert(op, net::OpTarget::PipeRead { fut, buf });
            Ok(())
        })?;
        if let Err(e) = res {
            // ReadFile can fail *synchronously* with ERROR_BROKEN_PIPE
            // (the writer already closed) instead of returning
            // ERROR_IO_PENDING — no op was posted, so no completion will
            // ever arrive via poll() to apply net.rs's usual
            // ERROR_BROKEN_PIPE-is-EOF translation. Apply it here instead,
            // resolving the future with b"" rather than raising, so this
            // path matches the async-completion path exactly (both from
            // _winpipes.py's point of view and stdlib's own
            // IocpProactor.recv() convention).
            if e.raw_os_error() == Some(net::ERROR_BROKEN_PIPE as i32) {
                fut_for_broken_pipe.call_method1(py, "set_result", (PyBytes::new(py, b""),))?;
                return Ok(());
            }
            return Err(PyErr::from(e));
        }
        Ok(())
    }

    /// Begin an overlapped write; `fut` resolves to the byte count
    /// written or an OSError. `data` is copied (pipe writes are not the
    /// hot path — see `IoBackend::post_pipe_write`).
    fn pipe_write(&self, handle: u64, data: &[u8], fut: Bound<'_, PyAny>) -> PyResult<()> {
        self.check_closed()?;
        let buf = data.to_vec();
        let fut: Py<PyAny> = fut.unbind();
        let res = self.with_net(|net, reactor| -> std::io::Result<()> {
            let op = reactor.backend_mut().post_pipe_write(handle as RawSocket, &buf)?;
            net.ops.insert(op, net::OpTarget::PipeWrite { fut, _buf: buf });
            Ok(())
        })?;
        res.map_err(PyErr::from)
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

    // ---- tasks & futures (R-050 native fast paths) -------------------------

    /// Give the core a reference to the `cadeloop.Loop` that wraps it, so
    /// `create_task` / `create_future` can pass `loop=` themselves.
    ///
    /// Idempotent and set-once: the facade calls it from `__init__`.
    fn set_owner(&self, owner: Bound<'_, PyAny>) {
        let _ = self.owner.set(owner.unbind());
    }

    /// `asyncio.Future(loop=<facade>)`, constructed by vectorcall.
    ///
    /// Bound onto the facade instance as `Loop.create_future`, so the
    /// attribute lookup lands on this native method directly -- no Python
    /// frame, no keyword-dict marshalling.
    fn create_future(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let owner = self.require_owner()?;
        taskfast::create_future(py, owner)
    }

    /// `asyncio.Task(coro, loop=<facade>)`, constructed by vectorcall.
    ///
    /// `name` / `context` are forwarded only when supplied. Anything this
    /// signature does not model -- a `**kwargs` a newer CPython grew, or
    /// debug mode, which wants the `_source_traceback` trim the facade
    /// does -- falls back to the facade's own `create_task`. The facade
    /// unbinds this method entirely while a task factory is installed, so
    /// the factory branch is not reachable from here.
    #[pyo3(signature = (coro, *, name=None, context=None, **kwargs))]
    fn create_task(
        &self,
        py: Python<'_>,
        coro: Bound<'_, PyAny>,
        name: Option<Bound<'_, PyAny>>,
        context: Option<Bound<'_, PyAny>>,
        kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.check_closed()?;
        let owner = self.require_owner()?;
        let extra = kwargs.as_ref().is_some_and(|kw| !kw.is_empty());
        if extra || self.debug.load(Ordering::Relaxed) {
            return self.create_task_via_facade(py, owner, coro, name, context, kwargs);
        }
        let name = name.filter(|n| !n.is_none());
        let context = context.filter(|c| !c.is_none());
        taskfast::create_task(py, owner, &coro, name.as_ref(), context.as_ref())
    }

    // ---- gc ----------------------------------------------------------------

    /// Let the cycle collector see the facade the core points back at.
    ///
    /// `Loop.__dict__` holds the core; the core holds the facade -- both
    /// through `owner` and through the three hooks, each of which is a
    /// bound method of the facade. Without a `tp_traverse` the collector
    /// cannot see that edge, so every closed loop stayed unreachable *and*
    /// uncollectable: a process that creates and closes loops (a test
    /// suite, a worker that restarts its loop) accumulated one dead
    /// `Loop` per cycle forever. Being traversable is enough to fix it --
    /// the collector breaks the cycle through the facade's own `tp_clear`,
    /// which clears its `__dict__`.
    ///
    /// Only the slots reachable without borrowing the state cell are
    /// visited. Under-reporting in `tp_traverse` costs at most a missed
    /// collection, never a premature free -- and by the time a loop is
    /// garbage, `close()` has already emptied the cell.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        for slot in [&self.owner, &self.error_hook, &self.net_error_hook, &self.slow_callback_hook] {
            if let Some(obj) = slot.get() {
                visit.call(obj)?;
            }
        }
        Ok(())
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
        let (stats, ready, timers, netstats, diag, live_name) = self.state.with(|st| {
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
                    st.net.stats_accept_starved,
                    st.net.stats_pipeline_pauses,
                ),
                st.reactor.backend_mut().diag(),
                // Live, not the construction-time cache: RIO downgrades its
                // name to "rio-polling" if RIONotify starts failing mid-run.
                st.reactor.backend_name(),
            )
        })?;
        let d = PyDict::new(py);
        d.set_item("backend", live_name)?;
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
        d.set_item("accept_starved", netstats.6)?;
        d.set_item("pipeline_pauses", netstats.7)?;
        if let Some((notifies, reaps)) = diag {
            d.set_item("rio_notifies", notifies)?;
            d.set_item("rio_watchdog_reaps", reaps)?;
        }
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
        kind: net::ListenerKind,
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
        let lid = self.with_net(|net, reactor| -> std::io::Result<u64> {
            reactor.backend_mut().register_socket(sock)?;
            let lid = net::listener_create(net, sock, kind, accept_pool);
            if start {
                if let Err(e) = net::listener_start(net, reactor.backend_mut(), lid) {
                    // listener_create already inserted it and the socket is
                    // registered, but no lid reaches the caller -- so the
                    // port would stay bound (and a retry hit EADDRINUSE)
                    // until the whole loop closed. Undo both.
                    net::listener_teardown(net, reactor.backend_mut(), lid);
                    return Err(e);
                }
            }
            Ok(lid)
        })??;
        Ok((lid, name_obj, sock as u64))
    }
}

/// Create, bind, and listen a TCP socket (shared by tcp_listen /
/// http_listen).
fn bind_listen_socket(
    ip: &str,
    port: u16,
    backlog: i32,
    reuse_addr: bool,
    reuse_port: bool,
) -> PyResult<RawSocket> {
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
    Ok(sock)
}
