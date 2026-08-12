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
use pyo3::intern;
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

/// Invoke `handle.context.run(handle.callback, *handle.args)` via
/// vectorcall (R-054: `PyObject_VectorcallMethod`, zero intermediate
/// tuples/allocations for <= 6 positional args).
///
/// Fatal exceptions (KeyboardInterrupt / SystemExit) propagate as `Err` to
/// unwind `run_forever`, matching asyncio's `Handle._run` contract.
pub fn run_handle(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<DispatchOutcome> {
    let bound = obj.downcast::<Handle>().map_err(PyErr::from)?;
    let handle: &Handle = bound.get();
    if handle.is_cancelled() {
        return Ok(DispatchOutcome::Done);
    }

    const INLINE: usize = 8;
    let args = handle.args.bind(py);
    let nargs = args.len();
    let total = 2 + nargs; // context (receiver), callback, *args

    let mut inline: [*mut ffi::PyObject; INLINE] = [std::ptr::null_mut(); INLINE];
    let mut heap: Vec<*mut ffi::PyObject>;
    let stack: &mut [*mut ffi::PyObject] = if total <= INLINE {
        &mut inline[..total]
    } else {
        heap = vec![std::ptr::null_mut(); total];
        &mut heap[..]
    };

    stack[0] = handle.context.as_ptr();
    stack[1] = handle.callback.as_ptr();
    for i in 0..nargs {
        // Borrowed reference out of the owned tuple; the tuple outlives the
        // call because `handle` (frozen, refcounted) holds it.
        stack[2 + i] = unsafe { ffi::PyTuple_GET_ITEM(args.as_ptr(), i as ffi::Py_ssize_t) };
    }

    let name = intern!(py, "run");
    let result = unsafe {
        ffi::PyObject_VectorcallMethod(name.as_ptr(), stack.as_ptr(), total, std::ptr::null_mut())
    };
    if result.is_null() {
        let err = PyErr::fetch(py);
        if err.is_instance_of::<PyKeyboardInterrupt>(py) || err.is_instance_of::<PySystemExit>(py) {
            return Err(err);
        }
        return Ok(DispatchOutcome::Failed(err));
    }
    unsafe { ffi::Py_DECREF(result) };
    Ok(DispatchOutcome::Done)
}
