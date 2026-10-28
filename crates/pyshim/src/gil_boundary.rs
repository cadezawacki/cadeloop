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
//! 2. While the loop IS running (`owner == tid`), the loop thread keeps
//!    its borrow across exactly one GIL-released section: the reactor
//!    poll (R-021). Any other thread that holds the GIL therefore sees one
//!    of two things: the loop parked inside that borrow (`borrowed` set),
//!    or the loop thread outside every borrow, waiting for the GIL between
//!    Python-level steps. In the second case the GIL serializes access
//!    exactly as in (1), so `with` admits the caller -- asyncio's own
//!    loops accept calls like `call_soon`/`call_later`/`Future.set_result`
//!    from other threads outside debug mode, and so does uvloop. In the
//!    first case the state must not be touched: `with` raises
//!    RuntimeError and [`StateCell::try_with`] hands the closure back, so
//!    the scheduling calls route through the lock-free queue (R-022) and
//!    wake the loop instead.
//! 3. A borrow is never re-entered. A nested `with` -- reachable only
//!    when something inside a borrow ran Python code (a finalizer run by
//!    an allocation's GC pass, say) -- would alias the `&mut`; it is
//!    refused like the parked case above, which turns the would-be UB
//!    into a RuntimeError, or, for the scheduling calls, into a detour
//!    through the cross-thread queue.
//! 4. Cross-thread operations that never need the state (`stop`, `time`,
//!    `is_running`, `is_closed`) use atomics only.
//!
//! Why not a `Mutex`? Holding a lock across a GIL-released poll deadlocks
//! with any GIL-holding thread that blocks on the same lock (poll thread
//! needs the GIL back; GIL holder needs the lock). The affinity protocol
//! fails fast (raises) instead of blocking, so the deadlock is impossible
//! by construction.
//!
//! # nogil migration note
//!
//! Invariants 1 and 2 lean on the GIL. On free-threaded builds
//! `StateCell::with` would need a real (uncontended) lock or critical
//! section. Confining that change here is the point of this module.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// A non-zero identifier for the calling OS thread, unique among live
/// threads. 0 is reserved for "no owner".
///
/// Read straight from the OS rather than from a `thread_local!`: in a
/// cdylib a thread-local is a `__tls_get_addr` call, and this runs on
/// every entry into the state cell -- several times per scheduled
/// callback. `pthread_self` is the thread pointer (one load from `%fs`)
/// and `GetCurrentThreadId` one read of the TEB. Reuse of an id by a later
/// thread is harmless here: ownership is cleared before the owning thread
/// can exit (`OwnerGuard` drops when `run_forever` returns).
#[inline(always)]
pub fn current_tid() -> u64 {
    #[cfg(unix)]
    {
        extern "C" {
            fn pthread_self() -> usize;
        }
        // SAFETY: pthread_self has no preconditions and never fails.
        let t = unsafe { pthread_self() } as u64;
        debug_assert_ne!(t, 0);
        t
    }
    #[cfg(windows)]
    {
        extern "system" {
            fn GetCurrentThreadId() -> u32;
        }
        // SAFETY: no preconditions; thread ids are never 0.
        u64::from(unsafe { GetCurrentThreadId() })
    }
    #[cfg(not(any(unix, windows)))]
    {
        use std::sync::atomic::AtomicU64;
        static NEXT: AtomicU64 = AtomicU64::new(1);
        thread_local! {
            static TID: u64 = NEXT.fetch_add(1, Ordering::Relaxed);
        }
        TID.with(|t| *t)
    }
}

pub struct StateCell<S> {
    cell: UnsafeCell<S>,
    /// 0 when the loop is not running; the loop thread's tid while it is.
    owner: AtomicU64,
    /// A `with` borrow is live (on the loop thread, possibly parked in its
    /// GIL-released poll). Written only by GIL holders.
    borrowed: AtomicBool,
}

// SAFETY: see module docs — every access holds the GIL, `borrowed`
// excludes the parked loop thread and re-entry, and `with` never yields to
// Python while the mutable borrow is live except across the poll, during
// which `borrowed` stays set.
unsafe impl<S: Send> Sync for StateCell<S> {}
unsafe impl<S: Send> Send for StateCell<S> {}

impl<S> StateCell<S> {
    pub fn new(state: S) -> Self {
        StateCell { cell: UnsafeCell::new(state), owner: AtomicU64::new(0), borrowed: AtomicBool::new(false) }
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
    /// is sound because the borrow stays marked live for its duration.
    #[inline]
    pub fn with<R>(&self, f: impl FnOnce(&mut S) -> R) -> PyResult<R> {
        self.try_with(f).map_err(|_| {
            PyRuntimeError::new_err(
                "Non-thread-safe operation invoked on an event loop other than the current one \
                 (the loop is waiting for I/O; use loop.call_soon_threadsafe())",
            )
        })
    }

    /// [`StateCell::with`], or `Err(f)` -- `f` not run -- when the state is
    /// borrowed already: the loop is parked in its poll and the caller is
    /// another thread, or the caller re-entered a live borrow.
    ///
    /// The test is one relaxed load. `borrowed` is only ever written by
    /// GIL holders (the parked loop thread wrote it before releasing the
    /// GIL), and every reader holds the GIL, whose handoff orders those
    /// writes before this read.
    #[inline]
    pub fn try_with<F: FnOnce(&mut S) -> R, R>(&self, f: F) -> Result<R, F> {
        if self.borrowed.load(Ordering::Relaxed) {
            return Err(f);
        }
        // A plain store, not a swap: no other writer can race it (see
        // above), and a locked RMW on every state access was measurable.
        self.borrowed.store(true, Ordering::Relaxed);
        // SAFETY: module-level protocol: the GIL serializes every
        // borrower, and `borrowed` excludes the parked loop thread and
        // re-entry.
        let r = f(unsafe { &mut *self.cell.get() });
        self.borrowed.store(false, Ordering::Relaxed);
        Ok(r)
    }

    /// The loop is running on a thread other than the caller's (asyncio's
    /// debug-mode `_check_thread`).
    pub fn owned_by_other_thread(&self) -> bool {
        let owner = self.owner.load(Ordering::Acquire);
        owner != 0 && owner != current_tid()
    }

    /// Shared access for the cycle collector's `tp_traverse`, or None when
    /// that cannot be sound: the loop is running (its thread may hold the
    /// `&mut` from `with` across the GIL-released poll), or a `with`
    /// borrow is live on this thread (an allocation inside it triggered
    /// the collection). Returning None makes the traverse under-report,
    /// which can only delay a collection, never cause a premature free.
    pub fn peek_for_gc(&self) -> Option<&S> {
        if self.owner.load(Ordering::Acquire) != 0 || self.borrowed.load(Ordering::Relaxed) {
            return None;
        }
        // SAFETY: not running and not borrowed; traverse runs with the
        // GIL held, so no `with` can start until it returns.
        Some(unsafe { &*self.cell.get() })
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
