//! `Handle` / `TimerHandle` pyclasses and the vectorcall dispatch path
//! (R-054).
//!
//! # Reference release instead of GC tracking
//!
//! A handle holds its callback, arguments and context only while the loop
//! owns it. The moment the loop gives a handle up -- a one-shot callback
//! has been dispatched, any handle is cancelled, a reader/writer watch is
//! removed, the loop closes or is deallocated -- those references are
//! released. A handle that still references anything is therefore always
//! reachable through its loop, so it can never be part of a garbage
//! cycle, and the handle types need no cycle-collector support.
//!
//! That matters twice over. Without it the standard pattern
//! `self._timeout = loop.call_later(t, self._on_timeout)` -- the cycle
//! object -> handle -> bound method -> object -- was uncollectable, and
//! every object that armed a timer on itself leaked for the life of the
//! process, fired or cancelled. And GC-tracking the handles instead (what
//! asyncio and uvloop do) makes the collector traverse every pending
//! handle on every collection, which measured as a ~35% loss on loops
//! with many callbacks or timers in flight. Releasing on cancel is also
//! exactly what asyncio's `Handle.cancel()` does (`_callback = None`), so
//! a cancelled timer's target is freed at once rather than when its heap
//! tombstone finally surfaces.
//!
//! The mutable part lives in a [`GilCell`]: CPython 3.11's GIL serializes
//! every access, and no borrow is ever held across a call into Python --
//! the same soundness argument `gil_boundary` makes for the loop state.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cadeloop_core::timer::TimerPayload;
use pyo3::basic::CompareOp;
use pyo3::exceptions::{PyKeyboardInterrupt, PySystemExit};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3::{PyTraverseError, PyVisit};

use crate::ctxsnap::Scratch;

/// A callback's positional arguments, stored inline.
///
/// Zero, one and two arguments cover the scheduling the loop does on its
/// own behalf -- a Task step (none), a Future done-callback (the future),
/// `call_soon(cb, x)` -- and storing them inline is what lets the native
/// `call_soon` skip building an argument tuple per call: the tuple was a
/// GC-tracked allocation made only to be unpacked again at dispatch.
pub(crate) enum CallArgs {
    Zero,
    One(Py<PyAny>),
    Two(Py<PyAny>, Py<PyAny>),
    Many(Py<PyTuple>),
}

impl CallArgs {
    /// From a tuple the caller already has (PyO3 `*args` paths).
    pub(crate) fn from_tuple(args: &Bound<'_, PyTuple>) -> CallArgs {
        match args.len() {
            0 => CallArgs::Zero,
            1 => CallArgs::One(unsafe { borrowed_item(args, 0) }),
            2 => CallArgs::Two(unsafe { borrowed_item(args, 0) }, unsafe { borrowed_item(args, 1) }),
            _ => CallArgs::Many(args.clone().unbind()),
        }
    }

    /// From a vectorcall argument array (the native FASTCALL entry points).
    ///
    /// # Safety
    /// `args` must point to `n` valid, borrowed object pointers.
    pub(crate) unsafe fn from_slice(
        py: Python<'_>,
        args: *const *mut ffi::PyObject,
        n: usize,
    ) -> PyResult<CallArgs> {
        unsafe {
            Ok(match n {
                0 => CallArgs::Zero,
                1 => CallArgs::One(Bound::from_borrowed_ptr(py, *args).unbind()),
                2 => CallArgs::Two(
                    Bound::from_borrowed_ptr(py, *args).unbind(),
                    Bound::from_borrowed_ptr(py, *args.add(1)).unbind(),
                ),
                _ => {
                    let t = ffi::PyTuple_New(n as ffi::Py_ssize_t);
                    if t.is_null() {
                        return Err(PyErr::fetch(py));
                    }
                    for i in 0..n {
                        let item = *args.add(i);
                        ffi::Py_INCREF(item);
                        ffi::PyTuple_SET_ITEM(t, i as ffi::Py_ssize_t, item);
                    }
                    CallArgs::Many(Bound::from_owned_ptr(py, t).cast_into_unchecked::<PyTuple>().unbind())
                }
            })
        }
    }
}

/// # Safety
/// `i` must be in range for `t`.
unsafe fn borrowed_item(t: &Bound<'_, PyTuple>, i: usize) -> Py<PyAny> {
    unsafe {
        Bound::from_borrowed_ptr(t.py(), ffi::PyTuple_GET_ITEM(t.as_ptr(), i as ffi::Py_ssize_t)).unbind()
    }
}

/// Interior mutability serialized by the GIL.
///
/// Sound on CPython 3.11 because only the thread holding the GIL runs
/// Python or touches a handle, and [`GilCell::with`]'s closure must never
/// call into Python (so no second access can start while the `&mut` is
/// live). A free-threaded build would need a real lock here; this module
/// and `gil_boundary` are where that migration lives.
pub(crate) struct GilCell<T>(UnsafeCell<T>);

// SAFETY: see the type docs -- every access is made with the GIL held.
unsafe impl<T: Send> Sync for GilCell<T> {}

impl<T> GilCell<T> {
    fn new(v: T) -> Self {
        GilCell(UnsafeCell::new(v))
    }

    /// Exclusive access for the duration of `f`, which must not call into
    /// Python (no `repr`, no decref that could reach `__del__`: move such
    /// values out and drop them after `with` returns).
    #[inline(always)]
    fn with<R>(&self, _py: Python<'_>, f: impl FnOnce(&mut T) -> R) -> R {
        // SAFETY: the GIL is held (`_py`) and `f` cannot re-enter.
        f(unsafe { &mut *self.0.get() })
    }
}

/// What a handle references while the loop owns it.
///
/// Five pointer-sized slots, nothing else: handles are allocated per
/// `call_soon`, and with many in flight their size is memory traffic. (The
/// fifth slot is free in practice: it takes a Handle from 56 to 64 bytes
/// and a TimerHandle from 72 to 80, the pymalloc size classes both
/// already occupied.)
///
/// * arguments: none (`a0`, `a1` empty), one (`a0`), two (`a0`, `a1`), or
///   more (`a0` = the argument tuple, `a1` = [`many_marker`]);
/// * released (module docs): `context` is None. In debug mode `callback`
///   then holds the released callback's formatted source (a `str`) for
///   `__repr__`, as asyncio's debug-mode `cancel()` saves `_repr`;
/// * `source_tb`: asyncio's `_source_traceback`, captured in debug mode
///   only. Not released: asyncio keeps it for the handle's life, and it
///   references frame summaries, never the objects a cycle runs through.
struct Refs {
    callback: Option<Py<PyAny>>,
    context: Option<Py<PyAny>>,
    a0: Option<Py<PyAny>>,
    a1: Option<Py<PyAny>>,
    source_tb: Option<Py<PyAny>>,
}

/// A private sentinel in `Refs::a1` meaning "`a0` is the argument tuple".
/// Unique to this module, so no real second argument can be mistaken for
/// it (the empty tuple, say, could).
static MANY_MARKER: pyo3::sync::PyOnceLock<Py<PyAny>> = pyo3::sync::PyOnceLock::new();

fn many_marker(py: Python<'_>) -> PyResult<&'static Py<PyAny>> {
    MANY_MARKER.get_or_try_init(py, || Ok(pyo3::types::PyList::empty(py).into_any().unbind()))
}

impl Refs {
    fn new(
        py: Python<'_>,
        callback: Py<PyAny>,
        args: CallArgs,
        context: Py<PyAny>,
        source_tb: Option<Py<PyAny>>,
    ) -> PyResult<Refs> {
        let (a0, a1) = match args {
            CallArgs::Zero => (None, None),
            CallArgs::One(a) => (Some(a), None),
            CallArgs::Two(a, b) => (Some(a), Some(b)),
            CallArgs::Many(t) => (Some(t.into_any()), Some(many_marker(py)?.clone_ref(py))),
        };
        Ok(Refs { callback: Some(callback), context: Some(context), a0, a1, source_tb })
    }

    /// The arguments as asyncio's `Handle._args` tuple (None once released).
    fn args_tuple(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if self.context.is_none() {
            return Ok(None);
        }
        if self.is_many(py) {
            return Ok(self.a0.as_ref().map(|t| t.clone_ref(py)));
        }
        let items: Vec<&Py<PyAny>> = [&self.a0, &self.a1].into_iter().flatten().collect();
        Ok(Some(PyTuple::new(py, items)?.into_any().unbind()))
    }

    #[inline]
    fn is_many(&self, py: Python<'_>) -> bool {
        match (&self.a1, MANY_MARKER.get(py)) {
            (Some(a1), Some(m)) => a1.as_ptr() == m.as_ptr(),
            _ => false,
        }
    }
}

/// asyncio.Handle equivalent: a scheduled callback plus its contextvars
/// context.
#[pyclass(frozen, subclass, module = "cadeloop._core")]
pub struct Handle {
    refs: GilCell<Refs>,
    cancelled: AtomicBool,
    /// asyncio's `TimerHandle._scheduled`: the timer heap still holds this
    /// handle. Only meaningful for timers; cleared by the heap when the
    /// handle leaves it (fired or discarded), so a `cancel()` after that
    /// point reports no tombstone.
    scheduled: AtomicBool,
    /// A reader/writer watch: dispatched again on every readiness event,
    /// so dispatch must not release its references (removal does).
    persistent: bool,
    /// Created in debug mode: keep a repr when references are released.
    debug: bool,
    /// `context` is a shared snapshot (`ctxsnap`): the callback runs in a
    /// scratch context with its variables, never in the snapshot itself.
    /// Set for callbacks scheduled without an explicit context; an explicit
    /// one is entered as given, as asyncio does.
    snapshot: bool,
}

impl Handle {
    pub(crate) fn new(
        py: Python<'_>,
        callback: Py<PyAny>,
        args: CallArgs,
        (context, snapshot): (Py<PyAny>, bool),
        debug: bool,
        source_tb: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        Ok(Handle {
            refs: GilCell::new(Refs::new(py, callback, args, context, source_tb)?),
            cancelled: AtomicBool::new(false),
            scheduled: AtomicBool::new(false),
            persistent: false,
            debug,
            snapshot,
        })
    }

    /// A readiness-watch handle (see `persistent`).
    ///
    /// A watch gets a context of its own even when it was scheduled with a
    /// shared snapshot: asyncio runs every readiness callback of one watch
    /// in the same context, so a variable a reader sets is still there on
    /// its next call -- which a scratch context per dispatch would lose.
    pub(crate) fn persistent(mut self, py: Python<'_>) -> PyResult<Self> {
        self.persistent = true;
        if self.snapshot {
            let r = self.refs.0.get_mut();
            if let Some(snap) = r.context.as_ref() {
                // SAFETY: a snapshot handle's context is a live Context.
                r.context = Some(unsafe { crate::ctxsnap::own_copy(py, snap.as_ptr())? });
            }
            self.snapshot = false;
        }
        Ok(self)
    }

    /// Returns true when this call newly cancelled the handle.
    #[inline]
    pub fn do_cancel(&self) -> bool {
        // Load first: re-cancelling (common in cleanup paths) then costs
        // no locked instruction.
        !self.cancelled.load(Ordering::Relaxed) && !self.cancelled.swap(true, Ordering::Relaxed)
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn mark_scheduled(&self) {
        self.scheduled.store(true, Ordering::Relaxed);
    }

    #[inline]
    fn is_scheduled(&self) -> bool {
        self.scheduled.load(Ordering::Relaxed)
    }

    /// Drop the callback, arguments and context: the loop is done with
    /// this handle (see the module docs). Must be called OUTSIDE the loop
    /// state cell -- the drops can run arbitrary Python. Idempotent.
    pub(crate) fn release(&self, py: Python<'_>) {
        // In debug mode the callback's formatted source survives the
        // release (asyncio's debug cancel() saves `_repr` the same way).
        // Formatting is Python code, so it runs before the cell is entered.
        let repr = if self.debug {
            match self.callback_and_args(py) {
                Ok(Some((cb, args))) => {
                    let text = format_callback_source(py, cb.bind(py), args.bind(py))
                        .unwrap_or_else(|_| "<callback>".to_owned());
                    Some(pyo3::types::PyString::new(py, &text).into_any().unbind())
                }
                _ => None, // released already
            }
        } else {
            None
        };
        let taken = self.refs.with(py, |r| {
            r.context.as_ref()?; // released already
            let cb = std::mem::replace(&mut r.callback, repr);
            Some((cb, r.context.take(), r.a0.take(), r.a1.take()))
        });
        drop(taken);
    }

    /// Report what this handle references to the cycle collector, on
    /// behalf of the loop that owns it (see `CoreLoop::__traverse__`).
    ///
    /// The callback and arguments only. The context is left out on
    /// purpose: visiting a per-handle copy (what every handle held before
    /// snapshots were shared, and what explicit-context handles still
    /// hold) made full collections of an idle loop with many callbacks
    /// queued twice as expensive -- and a reference cycle that runs
    /// through a context, rather than through the callback or an argument,
    /// is vanishingly rare. (The loop reports its cached shared snapshot
    /// itself.) Under-reporting can only delay a collection, never free
    /// anything early.
    pub(crate) fn visit_refs(&self, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
        // SAFETY: traversal runs with the GIL held and cannot overlap a
        // `GilCell::with` (whose closures never reach Python, so never
        // trigger a collection); a shared read is all this does.
        let r = unsafe { &*self.refs.0.get() };
        if r.context.is_none() {
            return Ok(()); // released: references nothing that matters
        }
        for slot in [&r.callback, &r.a0, &r.a1].into_iter().flatten() {
            visit.call(slot)?;
        }
        Ok(())
    }

    /// `(callback, args tuple)` while the handle still holds them.
    fn callback_and_args(&self, py: Python<'_>) -> PyResult<Option<(Py<PyAny>, Py<PyAny>)>> {
        let (cb, args) = self.refs.with(py, |r| {
            if r.context.is_none() {
                return Ok::<_, PyErr>((None, None));
            }
            Ok((r.callback.as_ref().map(|c| c.clone_ref(py)), r.args_tuple(py)?))
        })?;
        Ok(cb.zip(args))
    }

    /// asyncio's `Handle._repr_info()`, joined: `Handle`/`TimerHandle`,
    /// `cancelled`, `when=` (timers), the callback's formatted source, and
    /// `created at file:line` when a source traceback was captured.
    fn repr_text(&self, py: Python<'_>, kind: &str, when: Option<f64>) -> PyResult<String> {
        let mut info = vec![kind.to_owned()];
        if self.is_cancelled() {
            info.push("cancelled".to_owned());
        }
        if let Some(when) = when {
            info.push(format!("when={}", pyo3::types::PyFloat::new(py, when).repr()?));
        }
        let (saved, tb) = self.refs.with(py, |r| {
            let saved = match (&r.context, &r.callback) {
                (None, Some(text)) => Some(text.clone_ref(py)), // debug-mode saved source
                _ => None,
            };
            (saved, r.source_tb.as_ref().map(|t| t.clone_ref(py)))
        });
        if let Some(text) = saved {
            info.push(text.bind(py).str()?.to_string());
        } else if let Some((cb, args)) = self.callback_and_args(py)? {
            info.push(format_callback_source(py, cb.bind(py), args.bind(py))?);
        }
        if let Some(tb) = tb {
            let tb = tb.bind(py);
            if tb.len()? > 0 {
                let frame = tb.get_item(-1)?;
                info.push(format!("created at {}:{}", frame.get_item(0)?.str()?, frame.get_item(1)?.str()?));
            }
        }
        Ok(format!("<{}>", info.join(" ")))
    }

    /// Release after a dispatch, unless this is a persistent watch.
    #[inline]
    pub(crate) fn release_after_dispatch(&self, py: Python<'_>) {
        if !self.persistent {
            self.release(py);
        }
    }
}

/// A strong reference to a [`Handle`]: the token type of the reactor's
/// ready queue, timer heap and cross-thread queue.
///
/// A newtype only because the coherence rules forbid implementing the
/// core crate's [`TimerPayload`] for `Py<Handle>` directly.
#[repr(transparent)]
pub(crate) struct HandleRef(pub(crate) Py<Handle>);

impl HandleRef {
    #[inline(always)]
    pub(crate) fn get(&self) -> &Handle {
        self.0.get()
    }

    #[inline(always)]
    pub(crate) fn into_ptr(self) -> *mut ffi::PyObject {
        self.0.into_ptr()
    }
}

/// The reactor's view of a handle sitting in the timer heap. Everything
/// here runs with the GIL held (in-cell), so relaxed atomics suffice: the
/// GIL hand-off already orders a `cancel()` made by another thread.
impl TimerPayload for HandleRef {
    #[inline(always)]
    fn is_cancelled(&self) -> bool {
        self.get().is_cancelled()
    }

    #[inline(always)]
    fn mark_unscheduled(&self) {
        self.get().scheduled.store(false, Ordering::Relaxed);
    }
}

#[pymethods]
impl Handle {
    fn cancel(&self, py: Python<'_>) {
        if self.do_cancel() {
            self.release(py);
        }
    }

    fn cancelled(&self) -> bool {
        self.is_cancelled()
    }

    // asyncio's private attributes, read by asyncio itself (the slow-
    // callback report's `_format_handle`, `_source_traceback` in debug
    // messages) and by introspection tools.

    #[getter]
    fn _callback(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.refs.with(py, |r| match r.context {
            Some(_) => r.callback.as_ref().map(|c| c.clone_ref(py)),
            None => None,
        })
    }

    #[getter]
    fn _args(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.refs.with(py, |r| r.args_tuple(py))
    }

    #[getter]
    fn _cancelled(&self) -> bool {
        self.is_cancelled()
    }

    #[getter]
    fn _source_traceback(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.refs.with(py, |r| r.source_tb.as_ref().map(|t| t.clone_ref(py)))
    }

    fn __repr__(slf: &Bound<'_, Self>) -> PyResult<String> {
        slf.get().repr_text(slf.py(), &slf.get_type().qualname()?.to_string(), None)
    }
}

/// `asyncio.format_helpers._format_callback_source(callback, args)`: the
/// callback's name with its arguments, plus its definition site.
fn format_callback_source(
    py: Python<'_>,
    callback: &Bound<'_, PyAny>,
    args: &Bound<'_, PyAny>,
) -> PyResult<String> {
    static FMT: pyo3::sync::PyOnceLock<Py<PyAny>> = pyo3::sync::PyOnceLock::new();
    let f = FMT.get_or_try_init(py, || {
        Ok::<_, PyErr>(py.import("asyncio.format_helpers")?.getattr("_format_callback_source")?.unbind())
    })?;
    f.call1(py, (callback, args))?.extract(py)
}

/// asyncio.TimerHandle equivalent. Cancellation tombstones the heap entry
/// (the heap reads the base `Handle`'s flag directly) and, while the timer
/// is still scheduled, tells the loop so compaction can keep the heap
/// small (R-053).
#[pyclass(frozen, extends = Handle, module = "cadeloop._core")]
pub struct TimerHandle {
    /// `when()`'s answer: the caller's own float (asyncio returns exactly
    /// the value `call_at` was given, or `time() + delay`), not the
    /// deadline rounded through integer nanoseconds and back.
    pub(crate) when: f64,
    /// The owning loop's tombstone counter. Only dereferenced while
    /// `scheduled` is set -- i.e. while the loop's timer heap holds this
    /// handle, which keeps the counter alive: the heap is drained, marking
    /// every entry unscheduled, before the loop state is dropped (see
    /// `LoopState`'s `Drop`). A pointer rather than a strong reference to
    /// the loop, so a handle the application keeps creates no
    /// handle -> loop edge for a reference cycle to form through.
    pub(crate) cancel_counter: *const AtomicUsize,
}

// SAFETY: `cancel_counter` is only dereferenced under the GIL while the
// handle is scheduled (see the field docs); the pointee is an atomic.
unsafe impl Send for TimerHandle {}
unsafe impl Sync for TimerHandle {}

#[pymethods]
impl TimerHandle {
    fn cancel(slf: PyRef<'_, Self>) {
        let py = slf.py();
        let base = slf.as_super();
        if base.do_cancel() {
            if base.is_scheduled() {
                // SAFETY: scheduled => the heap holds us => counter alive.
                unsafe { (*slf.cancel_counter).fetch_add(1, Ordering::Relaxed) };
            }
            base.release(py);
        }
    }

    /// Absolute deadline in `loop.time()` seconds.
    fn when(&self) -> f64 {
        self.when
    }

    #[getter]
    fn _when(&self) -> f64 {
        self.when
    }

    // asyncio.TimerHandle's ordering and hash: by `when`, with equality
    // also comparing callback, arguments and cancellation.

    fn __hash__(&self, py: Python<'_>) -> PyResult<isize> {
        pyo3::types::PyFloat::new(py, self.when).hash()
    }

    fn __richcmp__(slf: &Bound<'_, Self>, other: &Bound<'_, PyAny>, op: CompareOp) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let Ok(other) = other.cast::<TimerHandle>() else {
            return Ok(py.NotImplemented());
        };
        let (a, b) = (slf.get().when, other.get().when);
        let eq = || -> PyResult<bool> {
            if a != b {
                return Ok(false);
            }
            let (x, y) = (slf.as_super().get(), other.as_super().get());
            if x.is_cancelled() != y.is_cancelled() {
                return Ok(false);
            }
            let (xa, ya) = (x.callback_and_args(py)?, y.callback_and_args(py)?);
            match (xa, ya) {
                (None, None) => Ok(true),
                (Some((xc, xs)), Some((yc, ys))) => {
                    Ok(xc.bind(py).eq(yc.bind(py))? && xs.bind(py).eq(ys.bind(py))?)
                }
                _ => Ok(false),
            }
        };
        let r = match op {
            CompareOp::Lt => a < b,
            CompareOp::Gt => a > b,
            CompareOp::Le => a < b || eq()?,
            CompareOp::Ge => a > b || eq()?,
            CompareOp::Eq => eq()?,
            CompareOp::Ne => !eq()?,
        };
        Ok(pyo3::types::PyBool::new(py, r).to_owned().into_any().unbind())
    }

    fn __repr__(slf: &Bound<'_, Self>) -> PyResult<String> {
        let me = slf.borrow();
        me.as_super().repr_text(slf.py(), &slf.get_type().qualname()?.to_string(), Some(me.when))
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
/// method object) and vectorcall the callback with the inline arguments
/// laid out on the stack (`PY_VECTORCALL_ARGUMENTS_OFFSET`, so a bound
/// method callee can prepend `self` in place) -- zero copies, zero
/// intermediate objects for any arity.
///
/// The callback, arguments and context are held by strong references for
/// the duration of the call: the callback may cancel its own handle (or
/// remove its own watch), which releases the handle's references while
/// they are still in use.
///
/// Fatal exceptions (KeyboardInterrupt / SystemExit) propagate as `Err` to
/// unwind `run_forever`, matching asyncio's `Handle._run` contract.
#[inline]
pub fn run_handle(py: Python<'_>, handle: &Handle, scratch: &mut Scratch) -> PyResult<DispatchOutcome> {
    if handle.is_cancelled() {
        return Ok(DispatchOutcome::Done);
    }
    // Raw, owned pointers: [callback, context, arg0, arg1, many].
    let null = std::ptr::null_mut::<ffi::PyObject>();
    let (cb, ctx, a0, a1, many) = handle.refs.with(py, |r| unsafe {
        let own = |o: &Option<Py<PyAny>>| match o {
            Some(o) => {
                let p = o.as_ptr();
                ffi::Py_INCREF(p);
                p
            }
            None => null,
        };
        if r.context.is_none() {
            return (null, null, null, null, null); // released
        }
        if r.is_many(py) {
            (own(&r.callback), own(&r.context), null, null, own(&r.a0))
        } else {
            (own(&r.callback), own(&r.context), own(&r.a0), own(&r.a1), null)
        }
    });
    if cb.is_null() {
        // Released already (cancelled after the flag check above, or a
        // handle dispatched twice): nothing left to run.
        return Ok(DispatchOutcome::Done);
    }
    // A snapshot is never entered itself: the callback runs in a scratch
    // context with its variables (ctxsnap).
    let fresh = handle.snapshot;
    let ctx = if fresh {
        unsafe {
            let acquired = scratch.acquire(py, ctx);
            ffi::Py_DECREF(ctx); // the snapshot reference taken above
            match acquired {
                Ok(c) => c,
                Err(e) => {
                    for p in [cb, a0, a1, many] {
                        if !p.is_null() {
                            ffi::Py_DECREF(p);
                        }
                    }
                    return fatal_or_failed(py, e);
                }
            }
        }
    } else {
        ctx
    };

    let result = unsafe {
        let res = if ffi::PyContext_Enter(ctx) != 0 {
            // Entering can fail on the CALLER's account -- the usual way
            // is a Context already entered elsewhere, which is what
            // handing the same contextvars.Context to two overlapping
            // callbacks produces. Returning Err here made that unwind
            // run_forever and stop the loop, where the identical mistake
            // inside the callback is reported and survived. Same
            // treatment: fatal errors propagate, everything else is one
            // failed handle.
            Err(PyErr::fetch(py))
        } else {
            const OFFSET: usize = ffi::PY_VECTORCALL_ARGUMENTS_OFFSET;
            let res = if !many.is_null() {
                // The tuple's ob_item array is the vectorcall args in place.
                let n = ffi::PyTuple_GET_SIZE(many);
                let items = std::ptr::addr_of!((*many.cast::<ffi::PyTupleObject>()).ob_item)
                    .cast::<*mut ffi::PyObject>();
                ffi::PyObject_Vectorcall(cb, items, n as usize, null)
            } else if a0.is_null() {
                ffi::compat::PyObject_CallNoArgs(cb)
            } else if a1.is_null() {
                let mut stack = [null, a0];
                ffi::PyObject_Vectorcall(cb, stack.as_mut_ptr().add(1), 1 | OFFSET, null)
            } else {
                let mut stack = [null, a0, a1];
                ffi::PyObject_Vectorcall(cb, stack.as_mut_ptr().add(1), 2 | OFFSET, null)
            };
            // Always restore the previous context, success or not.
            let exit_rc = ffi::PyContext_Exit(ctx);
            if exit_rc != 0 {
                if !res.is_null() {
                    ffi::Py_DECREF(res);
                }
                Err(PyErr::fetch(py))
            } else if res.is_null() {
                Err(PyErr::fetch(py))
            } else {
                ffi::Py_DECREF(res);
                Ok(())
            }
        };
        for p in [cb, a0, a1, many] {
            if !p.is_null() {
                ffi::Py_DECREF(p);
            }
        }
        if fresh {
            drop(scratch.give_back(py, ctx));
        } else {
            ffi::Py_DECREF(ctx);
        }
        res
    };
    match result {
        Ok(()) => Ok(DispatchOutcome::Done),
        Err(e) => fatal_or_failed(py, e),
    }
}
