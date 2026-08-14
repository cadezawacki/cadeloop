//! Vectorcall construction of `asyncio.Task` / `asyncio.Future` (R-050).
//!
//! `Loop.create_task()` / `Loop.create_future()` are on the hot path of
//! every task-spawning workload (and, through `asyncio.Queue`, of every
//! producer/consumer one). The Python facade's versions pay, per call, a
//! Python frame, a second frame for `_check_closed()`, and a keyword call
//! into the stdlib constructor that materialises an argument tuple and a
//! kwargs dict.
//!
//! The helpers here skip all of that: they call the *real, unmodified*
//! `asyncio.Task` / `asyncio.Future` classes through `PyObject_Vectorcall`
//! with a cached `kwnames` tuple, which is the same C-level calling
//! convention `handles.rs::run_handle` already uses for callback dispatch.
//! Nothing about the resulting objects differs from what the Python path
//! produces — this is a cheaper way to make the identical call, not a Task
//! reimplementation, so it carries no compatibility cost.
//!
//! Measured on CPython 3.11 (Linux, 20k constructions, min of 21 runs,
//! both sides called as a pre-bound method off the same instance so
//! neither pays a frame the other doesn't), across three runs:
//! `create_task` -23% to -27% (~180-200ns/call), `create_future` -9% to
//! -11% (~14-16ns/call). The `create_future` figure is the honest one:
//! most of what a vectorcall saves there is spent again on the PyO3
//! method boundary, so it is a small win, not the large one the shape of
//! the change suggests.

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyTuple;

static TASK_CLS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static FUTURE_CLS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
/// `("loop",)` — the kwnames tuple for the bare call shape.
static KW_LOOP: PyOnceLock<Py<PyTuple>> = PyOnceLock::new();
/// `("loop", "name")`.
static KW_LOOP_NAME: PyOnceLock<Py<PyTuple>> = PyOnceLock::new();
/// `("loop", "context")`.
static KW_LOOP_CONTEXT: PyOnceLock<Py<PyTuple>> = PyOnceLock::new();
/// `("loop", "name", "context")`.
static KW_LOOP_NAME_CONTEXT: PyOnceLock<Py<PyTuple>> = PyOnceLock::new();

fn asyncio_attr<'py>(
    py: Python<'py>,
    cell: &'static PyOnceLock<Py<PyAny>>,
    name: &str,
) -> PyResult<&'py Py<PyAny>> {
    // Resolve through `asyncio` (not `_asyncio`) so a runtime that ships
    // only the pure-Python implementation still works, and so that anyone
    // who has deliberately patched `asyncio.Task` sees their class used --
    // the Python path this replaces read the same attribute.
    cell.get_or_try_init(py, || -> PyResult<Py<PyAny>> { Ok(py.import("asyncio")?.getattr(name)?.unbind()) })
}

fn kwnames<'py>(
    py: Python<'py>,
    cell: &'static PyOnceLock<Py<PyTuple>>,
    names: &[&str],
) -> PyResult<&'py Py<PyTuple>> {
    cell.get_or_try_init(py, || -> PyResult<Py<PyTuple>> { Ok(PyTuple::new(py, names)?.unbind()) })
}

/// `PyObject_Vectorcall(callable, args[0..npos] ++ kw_values, npos, kwnames)`.
///
/// The vectorcall convention wants one flat array: `npos` positional
/// arguments followed by one value per name in `kwnames`. `args` must
/// already be laid out that way and must outlive the call (it does — the
/// callers hold owned references for the whole expression).
unsafe fn vectorcall(
    py: Python<'_>,
    callable: &Py<PyAny>,
    args: &[*mut ffi::PyObject],
    npos: usize,
    kwnames: &Py<PyTuple>,
) -> PyResult<Py<PyAny>> {
    let res = unsafe { ffi::PyObject_Vectorcall(callable.as_ptr(), args.as_ptr(), npos, kwnames.as_ptr()) };
    if res.is_null() {
        return Err(PyErr::fetch(py));
    }
    Ok(unsafe { Bound::from_owned_ptr(py, res) }.unbind())
}

/// `asyncio.Future(loop=owner)`.
pub(crate) fn create_future(py: Python<'_>, owner: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    let cls = asyncio_attr(py, &FUTURE_CLS, "Future")?;
    let kw = kwnames(py, &KW_LOOP, &["loop"])?;
    unsafe { vectorcall(py, cls, &[owner.as_ptr()], 0, kw) }
}

/// `asyncio.Task(coro, loop=owner[, name=..., context=...])`.
///
/// `name` / `context` are only forwarded when the caller actually supplied
/// them: passing `name=None, context=None` explicitly (as the Python path
/// did) costs ~96ns per call in argument marshalling for two values the
/// constructor then discards.
pub(crate) fn create_task(
    py: Python<'_>,
    owner: &Py<PyAny>,
    coro: &Bound<'_, PyAny>,
    name: Option<&Bound<'_, PyAny>>,
    context: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let cls = asyncio_attr(py, &TASK_CLS, "Task")?;
    let mut args: [*mut ffi::PyObject; 4] =
        [coro.as_ptr(), owner.as_ptr(), std::ptr::null_mut(), std::ptr::null_mut()];
    let mut n = 2;
    let kw = match (name, context) {
        (None, None) => kwnames(py, &KW_LOOP, &["loop"])?,
        (Some(nm), None) => {
            args[n] = nm.as_ptr();
            n += 1;
            kwnames(py, &KW_LOOP_NAME, &["loop", "name"])?
        }
        (None, Some(ctx)) => {
            args[n] = ctx.as_ptr();
            n += 1;
            kwnames(py, &KW_LOOP_CONTEXT, &["loop", "context"])?
        }
        (Some(nm), Some(ctx)) => {
            args[n] = nm.as_ptr();
            args[n + 1] = ctx.as_ptr();
            n += 2;
            kwnames(py, &KW_LOOP_NAME_CONTEXT, &["loop", "name", "context"])?
        }
    };
    unsafe { vectorcall(py, cls, &args[..n], 1, kw) }
}
