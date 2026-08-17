//! `Handle` / `TimerHandle` pyclasses and the vectorcall dispatch path
//! (R-054).
//!
//! Both classes are frozen (immutable Python-visible state); mutability is
//! atomics only, so they are safely shareable and never take pyo3 borrows
//! at dispatch time.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use cadeloop_core::timer::TimerToken;
use pyo3::exceptions::{PyKeyboardInterrupt, PySystemExit};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

/// asyncio.Handle equivalent: a scheduled callback plus its contextvars
/// context.
#[pyclass(frozen, subclass, module = "cadeloop._core")]
pub struct Handle {
    pub callback: Py<PyAny>,
    pub args: Py<PyTuple>,
    pub context: Py<PyAny>,
    cancelled: AtomicBool,
}

impl Handle {
    pub fn new(callback: Py<PyAny>, args: Py<PyTuple>, context: Py<PyAny>) -> Self {
        Handle { callback, args, context, cancelled: AtomicBool::new(false) }
    }

    /// Returns true when this call newly cancelled the handle.
    pub fn do_cancel(&self) -> bool {
        !self.cancelled.swap(true, Ordering::AcqRel)
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[pymethods]
impl Handle {
    fn cancel(&self) {
        self.do_cancel();
    }

    fn cancelled(&self) -> bool {
        self.is_cancelled()
    }

    fn __repr__(slf: &Bound<'_, Self>) -> PyResult<String> {
        let h = slf.get();
        let cb = h.callback.bind(slf.py()).repr()?;
        let state = if h.is_cancelled() { " cancelled" } else { "" };
        Ok(format!("<{}{} {}>", slf.get_type().qualname()?, state, cb))
    }
}

/// asyncio.TimerHandle equivalent. Cancellation additionally tombstones the
/// heap entry (via the shared [`TimerToken`]) and bumps the loop's
/// cancellation counter that drives compaction (R-053).
#[pyclass(frozen, extends = Handle, module = "cadeloop._core")]
pub struct TimerHandle {
    pub token: Arc<TimerToken>,
    /// Absolute deadline, nanoseconds since loop epoch.
    pub when_ns: u64,
    pub cancel_counter: Arc<AtomicUsize>,
}

#[pymethods]
impl TimerHandle {
    fn cancel(slf: PyRef<'_, Self>) {
        let newly = slf.as_super().do_cancel();
        if newly {
            slf.token.cancel();
            slf.cancel_counter.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Absolute deadline in `loop.time()` seconds.
    fn when(&self) -> f64 {
        cadeloop_core::time::ticks_to_secs_f64(self.when_ns)
    }
}

/// Outcome of running one handle.
pub enum DispatchOutcome {
    Done,
    /// Callback raised: (the handle, the exception). Non-fatal — report via
    /// the loop's exception handler.
    Failed(PyErr),
}

/// Split a callback-path error into "unwind the loop" and "report and keep
/// going", the way asyncio's `Handle._run` does: only KeyboardInterrupt
/// and SystemExit stop the loop.
fn fatal_or_failed(py: Python<'_>, err: PyErr) -> PyResult<DispatchOutcome> {
    if err.is_instance_of::<PyKeyboardInterrupt>(py) || err.is_instance_of::<PySystemExit>(py) {
        return Err(err);
    }
    Ok(DispatchOutcome::Failed(err))
}

/// Invoke `handle.callback(*handle.args)` inside `handle.context`.
///
/// Fast path adopted after profiling rloop/rsloop (R-054): enter/exit the
/// context directly via the C-API (no `context.run` attribute lookup or
/// method object) and vectorcall the callback using the argument tuple's
/// OWN item array (contiguous in a PyTupleObject) — zero copies, zero
/// intermediate objects for any arity.
///
/// Fatal exceptions (KeyboardInterrupt / SystemExit) propagate as `Err` to
/// unwind `run_forever`, matching asyncio's `Handle._run` contract.
pub fn run_handle(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<DispatchOutcome> {
    let bound = obj.cast::<Handle>().map_err(PyErr::from)?;
    let handle: &Handle = bound.get();
    if handle.is_cancelled() {
        return Ok(DispatchOutcome::Done);
    }

    let ctx = handle.context.as_ptr();
    let cb = handle.callback.as_ptr();
    let args = handle.args.as_ptr();

    let result = unsafe {
        if ffi::PyContext_Enter(ctx) != 0 {
            // Entering can fail on the CALLER's account -- the usual way
            // is a Context already entered elsewhere, which is what
            // handing the same contextvars.Context to two overlapping
            // callbacks produces. Returning Err here made that unwind
            // run_forever and stop the loop, where the identical mistake
            // inside the callback is reported and survived. Same
            // treatment: fatal errors propagate, everything else is one
            // failed handle.
            return fatal_or_failed(py, PyErr::fetch(py));
        }
        let n = ffi::PyTuple_GET_SIZE(args);
        let res = if n == 0 {
            ffi::compat::PyObject_CallNoArgs(cb)
        } else {
            // SAFETY: the tuple is owned by the frozen handle and outlives
            // the call; its ob_item array is the vectorcall args in place.
            let items =
                std::ptr::addr_of!((*args.cast::<ffi::PyTupleObject>()).ob_item).cast::<*mut ffi::PyObject>();
            ffi::PyObject_Vectorcall(cb, items, n as usize, std::ptr::null_mut())
        };
        // Always restore the previous context, success or not.
        let exit_rc = ffi::PyContext_Exit(ctx);
        if exit_rc != 0 && !res.is_null() {
            ffi::Py_DECREF(res);
            return Err(PyErr::fetch(py));
        }
        res
    };
    if result.is_null() {
        return fatal_or_failed(py, PyErr::fetch(py));
    }
    unsafe { ffi::Py_DECREF(result) };
    Ok(DispatchOutcome::Done)
}
