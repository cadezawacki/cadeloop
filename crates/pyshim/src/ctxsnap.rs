//! Context snapshots for callbacks scheduled without an explicit context.
//!
//! asyncio's `Handle` copies the current `contextvars.Context` when it is
//! created and runs its callback in that copy. Each copy is a GC-tracked
//! object, and a queue of pending callbacks used to hold one apiece: a
//! 200k-callback burst kept 200k contexts alive, and the young collections
//! they set off and the full ones their promotion fed were most of what
//! the burst cost. Timers scheduled by sleeping tasks did the same.
//!
//! A copy is only a holder for the variables, which are an immutable map
//! (CPython's HAMT) the copy shares with the context it was copied from.
//! So a handle keeps a *snapshot* instead: a context object nothing ever
//! enters, shared by every handle scheduled while the current context's
//! map was that same object. At dispatch the callback runs in a scratch
//! context with exactly the snapshot's variables ([`Scratch`]), which is
//! reused for the next callback only while that cannot be observed --
//! nothing else holds it, and the callback set nothing in it. Every
//! callback therefore still runs in a context of its own, as asyncio's
//! does; the queue just no longer holds an object per callback.
//!
//! Comparing maps by identity needs one field of CPython 3.11's private
//! `PyContext` struct. Its layout is checked through public APIs once, at
//! import ([`layout_ok`]); if the check fails, only empty contexts are
//! shared (detected with `len()`), which needs no private field at all.
//! (The empty-context case is rsloop's `capture_callback_context`;
//! sharing non-empty snapshots goes further.)
//!
//! Finding the current map without copying the current context first
//! reads one more field, `PyThreadState.context`, under its own import-time
//! check ([`tstate_ok`]): the copy and the immediate free of it were 140
//! instructions of every `call_soon`, 6% of a call_soon chain.

use std::os::raw::{c_int, c_ulong, c_void};
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};

use pyo3::ffi;
use pyo3::prelude::*;

/// CPython 3.11's `struct _pycontextobject` (Include/internal/pycore_context.h).
#[repr(C)]
struct RawContext {
    ob_base: ffi::PyObject,
    ctx_prev: *mut ffi::PyObject,
    ctx_vars: *mut ffi::PyObject,
    ctx_weakreflist: *mut ffi::PyObject,
    ctx_entered: c_int,
}

/// CPython 3.11's `struct _ts` (Include/cpython/pystate.h), through `id`.
/// A header-exposed struct (Cython reads its fields directly), so fixed
/// across 3.11.x; `c_ulong` is 32 bits on Windows, as there.
#[repr(C)]
struct RawThreadState {
    prev: *mut c_void,
    next: *mut c_void,
    interp: *mut ffi::PyInterpreterState,
    _initialized: c_int,
    _static: c_int,
    recursion_remaining: c_int,
    recursion_limit: c_int,
    recursion_headroom: c_int,
    tracing: c_int,
    tracing_what: c_int,
    cframe: *mut c_void,
    c_profilefunc: *mut c_void,
    c_tracefunc: *mut c_void,
    c_profileobj: *mut ffi::PyObject,
    c_traceobj: *mut ffi::PyObject,
    curexc_type: *mut ffi::PyObject,
    curexc_value: *mut ffi::PyObject,
    curexc_traceback: *mut ffi::PyObject,
    exc_info: *mut c_void,
    dict: *mut ffi::PyObject,
    gilstate_counter: c_int,
    async_exc: *mut ffi::PyObject,
    thread_id: c_ulong,
    native_thread_id: c_ulong,
    trash_delete_nesting: c_int,
    trash_delete_later: *mut ffi::PyObject,
    on_delete: *mut c_void,
    on_delete_data: *mut c_void,
    coroutine_origin_tracking_depth: c_int,
    async_gen_firstiter: *mut ffi::PyObject,
    async_gen_finalizer: *mut ffi::PyObject,
    context: *mut ffi::PyObject,
    context_ver: u64,
    id: u64,
}

/// 0 = unchecked, 1 = layout confirmed, 2 = layout unusable.
static LAYOUT: AtomicU8 = AtomicU8::new(0);
/// The same for [`RawThreadState`]; only consulted when `LAYOUT` is 1.
static TSTATE: AtomicU8 = AtomicU8::new(0);
/// The interpreter's empty variable map, which every empty context shares.
static EMPTY_VARS: AtomicPtr<ffi::PyObject> = AtomicPtr::new(std::ptr::null_mut());
/// A context with no variables that nothing ever enters: the snapshot every
/// callback scheduled from an empty context shares.
static EMPTY_SNAPSHOT: pyo3::sync::PyOnceLock<Py<PyAny>> = pyo3::sync::PyOnceLock::new();

/// # Safety
/// `ctx` must point to a live `contextvars.Context`, and `layout_ok()`.
#[inline(always)]
unsafe fn vars_of(ctx: *mut ffi::PyObject) -> *mut ffi::PyObject {
    unsafe { (*ctx.cast::<RawContext>()).ctx_vars }
}

/// Whether the private `PyContext` layout matches [`RawContext`].
#[inline(always)]
pub(crate) fn layout_ok() -> bool {
    LAYOUT.load(Ordering::Relaxed) == 1
}

/// Whether [`current_context`] can be used: both private layouts matched.
#[inline(always)]
fn tstate_ok() -> bool {
    TSTATE.load(Ordering::Relaxed) == 1
}

/// The calling thread's current context, borrowed, without copying it;
/// null when the thread never had one (equivalent to an empty one).
///
/// # Safety
/// The GIL must be held, and `tstate_ok()`.
#[inline(always)]
unsafe fn current_context() -> *mut ffi::PyObject {
    unsafe { (*ffi::PyThreadState_Get().cast::<RawThreadState>()).context }
}

/// Check the layouts through public APIs and record the empty map. Called
/// once from module init; anything unexpected disables the private-field
/// paths for good.
pub(crate) fn init(py: Python<'_>) -> PyResult<()> {
    let ok = unsafe { check_layout(py) }.unwrap_or(false);
    // A failed probe may leave an exception set; the fallback needs none.
    unsafe { ffi::PyErr_Clear() };
    LAYOUT.store(if ok { 1 } else { 2 }, Ordering::Relaxed);
    let ts_ok = ok && unsafe { check_tstate(py) }.unwrap_or(false);
    unsafe { ffi::PyErr_Clear() };
    TSTATE.store(if ts_ok { 1 } else { 2 }, Ordering::Relaxed);
    empty_snapshot(py)?;
    Ok(())
}

/// [`RawThreadState`] against the public accessors for the fields on
/// either side of `context` (`interp`, `dict`, `id`), and `context` itself
/// against a context entered and exited around the read.
unsafe fn check_tstate(py: Python<'_>) -> PyResult<bool> {
    unsafe {
        let ts = ffi::PyThreadState_Get();
        if ts.is_null() {
            return Ok(false);
        }
        let raw = ts.cast::<RawThreadState>();
        let dict = ffi::PyThreadState_GetDict(); // borrowed; creates it if need be
        if dict.is_null()
            || (*raw).dict != dict
            || (*raw).interp != ffi::PyThreadState_GetInterpreter(ts)
            || (*raw).id != ffi::PyThreadState_GetID(ts) as u64
        {
            return Ok(false);
        }
        let ctx = ffi::PyContext_New();
        if ctx.is_null() {
            return Err(PyErr::fetch(py));
        }
        let ctx = Bound::from_owned_ptr(py, ctx);
        let before = (*raw).context;
        if ffi::PyContext_Enter(ctx.as_ptr()) != 0 {
            return Err(PyErr::fetch(py));
        }
        let inside = (*raw).context;
        if ffi::PyContext_Exit(ctx.as_ptr()) != 0 {
            return Err(PyErr::fetch(py));
        }
        Ok(inside == ctx.as_ptr() && before != ctx.as_ptr() && (*raw).context == before)
    }
}

unsafe fn check_layout(py: Python<'_>) -> PyResult<bool> {
    unsafe {
        let new_ctx = || -> PyResult<Bound<'_, PyAny>> {
            let p = ffi::PyContext_New();
            if p.is_null() {
                return Err(PyErr::fetch(py));
            }
            Ok(Bound::from_owned_ptr(py, p))
        };
        let a = new_ctx()?;
        let b = new_ctx()?;
        if ffi::Py_TYPE(a.as_ptr()) != &raw mut ffi::PyContext_Type {
            return Ok(false);
        }
        let raw_a = &*a.as_ptr().cast::<RawContext>();
        let empty = raw_a.ctx_vars;
        // Two new contexts share the one empty map; neither is entered.
        if empty.is_null()
            || vars_of(b.as_ptr()) != empty
            || raw_a.ctx_entered != 0
            || !raw_a.ctx_prev.is_null()
        {
            return Ok(false);
        }
        // A copy shares its source's map.
        let c = ffi::PyContext_Copy(a.as_ptr());
        if c.is_null() {
            return Err(PyErr::fetch(py));
        }
        let c = Bound::from_owned_ptr(py, c);
        if vars_of(c.as_ptr()) != empty {
            return Ok(false);
        }
        // Entering sets ctx_entered; setting a variable inside replaces the
        // entered context's map and nothing else's.
        let var = ffi::PyContextVar_New(c"cadeloop_layout_probe".as_ptr(), std::ptr::null_mut());
        if var.is_null() {
            return Err(PyErr::fetch(py));
        }
        let var = Bound::from_owned_ptr(py, var);
        if ffi::PyContext_Enter(c.as_ptr()) != 0 {
            return Err(PyErr::fetch(py));
        }
        let entered = (*c.as_ptr().cast::<RawContext>()).ctx_entered == 1;
        let token = ffi::PyContextVar_Set(var.as_ptr(), py.None().as_ptr());
        let exited = ffi::PyContext_Exit(c.as_ptr());
        if token.is_null() || exited != 0 {
            return Err(PyErr::fetch(py));
        }
        drop(Bound::from_owned_ptr(py, token));
        let changed = vars_of(c.as_ptr());
        let ok = entered
            && (*c.as_ptr().cast::<RawContext>()).ctx_entered == 0
            && !changed.is_null()
            && changed != empty
            && vars_of(a.as_ptr()) == empty
            && ffi::PyObject_Size(c.as_ptr()) == 1
            && ffi::PyObject_Size(a.as_ptr()) == 0;
        if ok {
            EMPTY_VARS.store(empty, Ordering::Relaxed);
        }
        Ok(ok)
    }
}

fn empty_snapshot(py: Python<'_>) -> PyResult<&'static Py<PyAny>> {
    EMPTY_SNAPSHOT.get_or_try_init(py, || unsafe {
        let p = ffi::PyContext_New();
        if p.is_null() {
            return Err(PyErr::fetch(py));
        }
        Ok(Bound::from_owned_ptr(py, p).unbind())
    })
}

/// Whether context `ctx` has no variables. With the confirmed layout, one
/// pointer compare: CPython hands out a single empty map, and a context
/// whose variables were all reset gets that one back. (Were an empty map
/// ever another object, it would only be shared by identity like any
/// non-empty one -- still correct.) Without it, `len()`.
///
/// # Safety
/// `ctx` must point to a live `contextvars.Context`.
#[inline]
unsafe fn is_empty(ctx: *mut ffi::PyObject) -> bool {
    unsafe {
        if layout_ok() {
            return vars_of(ctx) == EMPTY_VARS.load(Ordering::Relaxed);
        }
        let n = ffi::PyObject_Size(ctx);
        if n < 0 {
            ffi::PyErr_Clear();
        }
        n == 0
    }
}

/// The last non-empty snapshot taken, per loop: reused while the current
/// context still has the same variable map. Held strongly, so the map it
/// names cannot be freed and its address reused while it is cached.
#[derive(Default)]
pub(crate) struct SnapCache(std::cell::UnsafeCell<Option<Py<PyAny>>>);

// SAFETY: every access holds the GIL (CPython 3.11), and no access calls
// into Python while it borrows the cell.
unsafe impl Sync for SnapCache {}
unsafe impl Send for SnapCache {}

impl SnapCache {
    /// The context a callback scheduled without one keeps, and whether it
    /// is a snapshot (run in a scratch copy) rather than a context of its
    /// own (run in directly, as asyncio does with its copy).
    ///
    /// Out of line: the busiest scheduling path -- a task's step, a
    /// future's callbacks -- passes its context explicitly and never gets
    /// here, and inlined into `make_handle` this grew that path past what
    /// stays in the instruction cache.
    #[inline(never)]
    pub(crate) fn capture(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, bool)> {
        unsafe {
            if tstate_ok() {
                // The current variables, read in place: when a snapshot of
                // them exists already, there is nothing to copy at all.
                let cur = current_context();
                let empty = EMPTY_VARS.load(Ordering::Relaxed);
                let vars = if cur.is_null() { empty } else { vars_of(cur) };
                if vars == empty {
                    return Ok((empty_snapshot(py)?.clone_ref(py), true));
                }
                if let Some(snap) = (*self.0.get()).as_ref() {
                    if vars_of(snap.as_ptr()) == vars {
                        return Ok((snap.clone_ref(py), true));
                    }
                }
            }
            let cur = ffi::PyContext_CopyCurrent();
            if cur.is_null() {
                return Err(PyErr::fetch(py));
            }
            if is_empty(cur) {
                // Back to CPython's context freelist: no GC allocation.
                ffi::Py_DECREF(cur);
                return Ok((empty_snapshot(py)?.clone_ref(py), true));
            }
            if !layout_ok() {
                return Ok((Bound::from_owned_ptr(py, cur).unbind(), false));
            }
            let vars = vars_of(cur);
            let slot = &mut *self.0.get();
            if let Some(snap) = slot.as_ref() {
                if vars_of(snap.as_ptr()) == vars {
                    ffi::Py_DECREF(cur);
                    return Ok((snap.clone_ref(py), true));
                }
            }
            let snap = Bound::from_owned_ptr(py, cur).unbind();
            // The one it replaces is dropped after the borrow ends: freeing
            // it can free variable values, and that can run any code.
            let old = slot.replace(snap.clone_ref(py));
            drop(old);
            Ok((snap, true))
        }
    }

    /// Drop the cached snapshot once no pending handle uses it, so it keeps
    /// nothing alive past the callbacks that needed it. Run once a tick.
    #[inline]
    pub(crate) fn trim(&self, _py: Python<'_>) {
        // SAFETY: GIL held; the taken value is dropped after the borrow.
        let old = unsafe {
            let slot = &mut *self.0.get();
            match slot.as_ref() {
                Some(snap) if ffi::Py_REFCNT(snap.as_ptr()) == 1 => slot.take(),
                _ => None,
            }
        };
        drop(old);
    }

    /// Drop the cached snapshot unconditionally (loop close / GC clear).
    pub(crate) fn clear(&self, _py: Python<'_>) {
        let old = unsafe { (*self.0.get()).take() };
        drop(old);
    }

    /// The cached snapshot, for the cycle collector.
    pub(crate) fn peek(&self) -> Option<&Py<PyAny>> {
        // SAFETY: traversal holds the GIL and cannot overlap a borrow.
        unsafe { (*self.0.get()).as_ref() }
    }
}

/// A context a snapshot handle's callback runs in: exactly the snapshot's
/// variables, held by nothing else. One is kept and reused while that is
/// unobservable -- its refcount is back to 1 (so it is neither entered
/// anywhere nor retained by the callback that ran in it) and its variables
/// are unchanged (that callback set nothing in it). Owned by `run_forever`'s
/// frame: dispatch only ever happens there.
pub(crate) struct Scratch {
    ctx: Option<Py<PyAny>>,
    /// The variable map `ctx` was made with (layout path only).
    vars: *mut ffi::PyObject,
}

impl Default for Scratch {
    fn default() -> Self {
        Scratch { ctx: None, vars: std::ptr::null_mut() }
    }
}

impl Scratch {
    /// An owned reference to a context with `snap`'s variables that
    /// nothing else holds.
    ///
    /// # Safety
    /// `snap` must point to a live snapshot context.
    pub(crate) unsafe fn acquire(
        &mut self,
        py: Python<'_>,
        snap: *mut ffi::PyObject,
    ) -> PyResult<*mut ffi::PyObject> {
        unsafe {
            let want = if layout_ok() { vars_of(snap) } else { std::ptr::null_mut() };
            if let Some(ctx) = self.ctx.take() {
                // Without the layout only the empty snapshot is ever shared,
                // and a kept scratch context is empty (see give_back).
                if !layout_ok() || self.vars == want {
                    return Ok(ctx.into_ptr());
                }
                // A context, so freeing it runs no code; dropped with a
                // plain decref, as dispatch drops everything it holds (a
                // `Py` drop's is-the-thread-attached probe is a TLS lookup).
                ffi::Py_DECREF(ctx.into_ptr());
            }
            let ctx = ffi::PyContext_Copy(snap);
            if ctx.is_null() {
                return Err(PyErr::fetch(py));
            }
            self.vars = want;
            Ok(ctx)
        }
    }

    /// Take back what `acquire` handed out, after the callback ran in it:
    /// kept for the next callback when that is unobservable, else released
    /// (with a plain decref, as dispatch releases everything it holds).
    ///
    /// # Safety
    /// `ctx` must be the owned reference `acquire` returned, and the GIL
    /// held.
    pub(crate) unsafe fn give_back(&mut self, py: Python<'_>, ctx: *mut ffi::PyObject) {
        unsafe {
            let unchanged = ffi::Py_REFCNT(ctx) == 1
                && if layout_ok() { vars_of(ctx) == self.vars } else { is_empty(ctx) };
            if unchanged {
                self.ctx = Some(Bound::from_owned_ptr(py, ctx).unbind());
            } else {
                // Freeing a context can free variable values, and that can
                // run any code -- harmless here: the scratch slot is empty
                // (acquire took it), so nothing re-entered can see it.
                ffi::Py_DECREF(ctx);
            }
        }
    }
}

/// A context of its own for a handle that holds a snapshot: what a
/// readiness watch needs, since asyncio runs every callback of one watch in
/// the same context.
///
/// # Safety
/// `snap` must point to a live snapshot context.
pub(crate) unsafe fn own_copy(py: Python<'_>, snap: *mut ffi::PyObject) -> PyResult<Py<PyAny>> {
    unsafe {
        let ctx = ffi::PyContext_Copy(snap);
        if ctx.is_null() {
            return Err(PyErr::fetch(py));
        }
        Ok(Bound::from_owned_ptr(py, ctx).unbind())
    }
}
