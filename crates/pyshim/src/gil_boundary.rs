//! GIL/threading boundary (R-010).
//!
//! Every assumption about the GIL is concentrated here so a later
//! 3.13+ free-threading port only has to revisit this module.
//!
//! # Invariants
//!
//! Loop state (`Reactor` and friends) is stored in a [`StateCell`] —
//! an `UnsafeCell` with a thread-affinity protocol instead of a lock:
//!
//! 1. While the loop is NOT running (`owner == 0`), state is only touched
//!    with the GIL held, and [`StateCell::with`] never calls back into
//!    Python while the borrow is live. Under the GIL (current assumption:
//!    exactly one thread runs Python at a time) this serializes all access.
//! 2. While the loop IS running (`owner == tid`), only the loop thread may
//!    enter `with`; any other thread gets `RuntimeError` — the same
//!    contract CPython documents ("almost all asyncio objects are not
//!    thread safe") and enforces via `BaseEventLoop._check_thread`.
//!    This is what makes it sound for the loop thread to release the GIL
//!    inside `with` (the reactor poll, R-021): no other thread can reach
//!    the state, GIL or not.
//! 3. Cross-thread operations (`call_soon_threadsafe`, `stop`, `time`,
//!    `is_running`, `is_closed`) never touch the `StateCell`; they use
//!    atomics and the lock-free MPSC queue (R-022) exclusively.
//!
//! Why not a `Mutex`? Holding a lock across a GIL-released poll deadlocks
//! with any GIL-holding thread that blocks on the same lock (poll thread
//! needs the GIL back; GIL holder needs the lock). The affinity protocol
//! fails fast (raises) instead of blocking, so the deadlock is impossible
//! by construction.
//!
//! # nogil migration note
//!
//! Invariant 1 leans on the GIL. On free-threaded builds `StateCell::with`
//! for a non-running loop would need a real (uncontended) lock or critical
//! section. Confining that change here is the point of this module.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// Process-unique small integer per OS thread (std's `ThreadId::as_u64` is
/// unstable; this is the stable equivalent). 0 is reserved for "no owner".
pub fn current_tid() -> u64 {
    use std::sync::atomic::AtomicU64;
    static NEXT: AtomicU64 = AtomicU64::new(1);
    thread_local! {
        static TID: u64 = NEXT.fetch_add(1, Ordering::Relaxed);
    }
    TID.with(|t| *t)
}

pub struct StateCell<S> {
    cell: UnsafeCell<S>,
    /// 0 when the loop is not running; the loop thread's tid while it is.
    owner: AtomicU64,
}

// SAFETY: see module docs — access is serialized by (GIL ∧ owner==0) or
// confined to the owner thread (owner==tid), and `with` never yields to
// Python while the mutable borrow is live.
unsafe impl<S: Send> Sync for StateCell<S> {}
unsafe impl<S: Send> Send for StateCell<S> {}

impl<S> StateCell<S> {
    pub fn new(state: S) -> Self {
        StateCell { cell: UnsafeCell::new(state), owner: AtomicU64::new(0) }
    }

    /// Claim ownership for the duration of `run_forever`. Returns a guard
    /// that clears ownership on drop (including on panic/exception paths).
    pub fn claim(&self) -> PyResult<OwnerGuard<'_, S>> {
        let me = current_tid();
        match self.owner.compare_exchange(0, me, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => Ok(OwnerGuard { cell: self }),
            Err(_) => Err(PyRuntimeError::new_err("This event loop is already running")),
        }
    }

    pub fn is_claimed(&self) -> bool {
        self.owner.load(Ordering::Acquire) != 0
    }

    /// Run `f` with exclusive access to the state.
    ///
    /// CONTRACT (upheld by all callers in this crate): `f` must not invoke
    /// arbitrary Python code — no user callbacks, no `Py::new`, nothing
    /// that can trigger GC or release/acquire the GIL — EXCEPT the single
    /// sanctioned `detach(poll)` inside `run_forever`'s tick, which
    /// is sound because the claiming thread has exclusive access.
    pub fn with<R>(&self, f: impl FnOnce(&mut S) -> R) -> PyResult<R> {
        let owner = self.owner.load(Ordering::Acquire);
        if owner != 0 && owner != current_tid() {
            return Err(PyRuntimeError::new_err(
                "Non-thread-safe operation invoked on an event loop other than the current one",
            ));
        }
        // SAFETY: module-level protocol (GIL ∧ owner==0) ∨ owner==tid.
        Ok(f(unsafe { &mut *self.cell.get() }))
    }
}

pub struct OwnerGuard<'a, S> {
    cell: &'a StateCell<S>,
}

impl<S> Drop for OwnerGuard<'_, S> {
    fn drop(&mut self) {
        self.cell.owner.store(0, Ordering::Release);
    }
}
