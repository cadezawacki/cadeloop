//! `CoreLoop`: the native scheduling core behind `cadeloop.Loop`.
//!
//! Owns the L1 reactor and implements the native fast paths of R-050
//! (call_soon / call_later / call_at / time / run_forever /
//! call_soon_threadsafe / stop / close / stats). The Python facade
//! (`python/cadeloop/loop.py`) completes the `asyncio.AbstractEventLoop`
//! surface (R-013) on top.
//!
//! Threading model: see `gil_boundary` (R-010). All Python callbacks run on
//! the loop thread with the GIL held; the kernel poll runs with the GIL
//! released, once per tick (R-021).

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use cadeloop_core::backend::{BackendKind, Wakeup};
use cadeloop_core::ready::CrossThreadQueue;
use cadeloop_core::reactor::{Reactor, ReactorConfig};
use cadeloop_core::time::{secs_f64_to_ticks, ticks_to_secs_f64, Ticks};
use cadeloop_core::timer::TimerToken;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::gil_boundary::StateCell;
use crate::handles::{run_handle, DispatchOutcome, Handle, TimerHandle};

struct LoopState {
    reactor: Reactor<Py<PyAny>>,
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
    /// Facade hook: `(handle, exception) -> None`, routes to
    /// `call_exception_handler`.
    error_hook: OnceLock<Py<PyAny>>,
    /// Facade hook: `(handle, seconds) -> None` for debug-mode slow
    /// callback warnings (R-142).
    slow_callback_hook: OnceLock<Py<PyAny>>,
    backend_name: &'static str,
}

impl CoreLoop {
    fn check_closed(&self) -> PyResult<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(PyRuntimeError::new_err("Event loop is closed"))
        } else {
            Ok(())
        }
    }

    /// Current loop time in ns: per-tick cache while running (R-061),
    /// fresh read otherwise.
    fn now_ns(&self) -> PyResult<Ticks> {
        if self.state.is_claimed() {
            Ok(self.cached_time_ns.load(Ordering::Acquire))
        } else {
            self.state.with(|st| st.reactor.now_fresh())
        }
    }

    /// Build a Handle for `callback(*args)` in `context` (copied from the
    /// current context when None). Created BEFORE entering the state
    /// critical section: object construction can trigger GC.
    fn make_handle(
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

    /// One full tick: prepare, poll (GIL released), finish, dispatch batch.
    fn tick(&self, py: Python<'_>) -> PyResult<()> {
        // Keep Ctrl-C responsive even when no Python code runs for a while.
        unsafe {
            if ffi::PyErr_CheckSignals() != 0 {
                return Err(PyErr::fetch(py));
            }
        }

        let stopping = self.stopping.load(Ordering::Acquire);
        let poll_result: std::io::Result<()> = self.state.with(|st| {
            st.reactor.prepare_tick();
            self.cached_time_ns.store(st.reactor.time_cached(), Ordering::Release);
            let timeout =
                if stopping { Duration::ZERO } else { st.reactor.poll_timeout() };
            let reactor = &mut st.reactor;
            // R-021: the only GIL release point; sound because `claim`
            // guarantees no other thread can enter this state.
            py.allow_threads(move || reactor.poll(timeout))
        })?;
        poll_result?;

        let graveyard: Vec<Py<PyAny>> = self.state.with(|st| {
            // I/O completions (empty pre-M1; transports consume them in M1).
            let _completions = st.reactor.finish_poll();
            self.cached_time_ns.store(st.reactor.time_cached(), Ordering::Release);
            st.reactor.take_graveyard()
        })?;
        // Cancelled-timer handles die here, outside the critical section.
        drop(graveyard);

        loop {
            let token = self.state.with(|st| st.reactor.pop_ready_batched())?;
            let Some(token) = token else { break };
            let debug = self.debug.load(Ordering::Relaxed);
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
    }
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
    #[pyo3(signature = (backend="auto", spin_us=20))]
    fn new(backend: &str, spin_us: u64) -> PyResult<Self> {
        let kind = BackendKind::parse(backend).ok_or_else(|| {
            PyValueError::new_err(format!(
                "invalid backend {backend:?}: expected 'auto', 'iocp' or 'rio'"
            ))
        })?;
        let cfg = ReactorConfig { backend: kind, spin_us, ..Default::default() };
        let reactor: Reactor<Py<PyAny>> = Reactor::new(cfg)?;
        let (xqueue, waker) = reactor.cross_thread_handles();
        let timer_cancels = reactor.timer_cancel_counter();
        let backend_name = reactor.backend_name();
        Ok(CoreLoop {
            state: StateCell::new(LoopState { reactor }),
            xqueue,
            waker,
            timer_cancels,
            closed: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            debug: AtomicBool::new(false),
            cached_time_ns: AtomicU64::new(0),
            error_hook: OnceLock::new(),
            slow_callback_hook: OnceLock::new(),
            backend_name,
        })
    }

    // ---- lifecycle -------------------------------------------------------

    fn run_forever(&self, py: Python<'_>) -> PyResult<()> {
        self.check_closed()?;
        let guard = self.state.claim()?;
        let result = loop {
            match self.tick(py) {
                Err(e) => break Err(e),
                Ok(()) => {
                    if self.stopping.load(Ordering::Acquire) {
                        break Ok(());
                    }
                }
            }
        };
        self.stopping.store(false, Ordering::Release);
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
        let pending = self.state.with(|st| st.reactor.clear_pending())?;
        drop(pending); // Python refs die outside the critical section.
        let _ = py;
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
    /// Never touches loop state.
    #[pyo3(signature = (callback, *args, context=None))]
    fn call_soon_threadsafe(
        &self,
        py: Python<'_>,
        callback: Bound<'_, PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<Handle>> {
        self.check_closed()?;
        let handle =
            self.make_handle(py, &callback, &args, context.as_ref(), "call_soon_threadsafe")?;
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

    // ---- hooks / config ----------------------------------------------------

    /// Facade wiring, called once from `Loop.__init__`.
    fn set_error_hook(&self, hook: Bound<'_, PyAny>) {
        let _ = self.error_hook.set(hook.unbind());
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
        let (stats, ready, timers) =
            self.state.with(|st| (st.reactor.stats.clone(), st.reactor.ready_len(), st.reactor.timers_len()))?;
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
        self.state
            .with(move |st| st.reactor.schedule_timer_with_token(deadline_ns, payload, token))?;
        Ok(handle)
    }
}
