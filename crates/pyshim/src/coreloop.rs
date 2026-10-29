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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use cadeloop_core::backend::{BackendKind, RawSocket, Wakeup};
use cadeloop_core::http::Limits;
use cadeloop_core::netsys;
use cadeloop_core::reactor::{Reactor, ReactorConfig};
use cadeloop_core::ready::CrossThreadQueue;
use cadeloop_core::time::{raw_now, secs_f64_to_ticks, ticks_to_secs_f64, Ticks, RAW_IS_PYTHON_MONOTONIC};
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError, PySystemExit, PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyString, PyTuple};
use pyo3::{PyTraverseError, PyVisit};

use crate::ctxsnap::{Scratch, SnapCache};
use crate::gil_boundary::StateCell;
use crate::handles::{run_handle, CallArgs, DispatchOutcome, Handle, HandleRef, TimerHandle};
use crate::net::{self, NetEvent, NetState, Transport};
use crate::taskfast;

pub(crate) struct LoopState {
    pub reactor: Reactor<HandleRef>,
    pub net: NetState,
    /// Scratch for per-tick completion copies (reused, no per-tick alloc).
    completions_scratch: Vec<cadeloop_core::backend::Completion>,
}

impl Drop for LoopState {
    /// A loop deallocated without `close()` still owns pending handles.
    /// Drain them through the normal path -- which marks every timer
    /// unscheduled, so a `TimerHandle` the application kept never touches
    /// the cancel counter this drop frees -- and release their references,
    /// so none of them is left holding a cycle (handles.rs module docs).
    fn drop(&mut self) {
        let mut pending: Vec<HandleRef> = self.reactor.clear_pending();
        pending.extend(std::mem::take(&mut self.net.ready_scratch).into_iter().map(HandleRef));
        pending.extend(self.net.readers.drain().map(|(_, h)| HandleRef(h)));
        pending.extend(self.net.writers.drain().map(|(_, h)| HandleRef(h)));
        if pending.is_empty() {
            return;
        }
        // Reached from the owning CoreLoop's tp_dealloc, which runs
        // attached; `attach` then takes its already-attached fast path.
        Python::attach(|py| {
            for h in &pending {
                h.get().release(py);
            }
            drop(pending);
        });
    }
}

#[pyclass(frozen, module = "cadeloop._core")]
pub struct CoreLoop {
    state: StateCell<LoopState>,
    xqueue: Arc<CrossThreadQueue<HandleRef>>,
    /// Context snapshot shared by callbacks scheduled without an explicit
    /// context (see `ctxsnap`).
    snaps: SnapCache,
    waker: Arc<dyn Wakeup>,
    /// Shared with the reactor: cancellations of still-scheduled timers,
    /// absorbed into the heap's tombstone accounting each tick.
    timer_cancels: Arc<AtomicUsize>,
    closed: AtomicBool,
    stopping: AtomicBool,
    debug: AtomicBool,
    /// Added to `raw_now()` to read the loop clock (0 on Linux, where the
    /// raw clock already is Python's monotonic one; see core::time).
    /// Immutable, so `loop.time()` needs no loop state and no atomics.
    clock_offset: i64,
    /// Tick counter for the throttled signal check. Loop-thread only:
    /// advanced with a plain load + store, never a locked `fetch_add` (an
    /// atomic RMW on every tick was the single hottest instruction of a
    /// pure-scheduling tick).
    tick_no: AtomicU64,
    /// Facade hook: `(handle, exception) -> None` for callback errors.
    error_hook: OnceLock<Py<PyAny>>,
    /// Facade hook: `(message, exception) -> None` for protocol/net errors.
    net_error_hook: OnceLock<Py<PyAny>>,
    /// Facade hook: `(handle, seconds) -> None` (R-142 slow callbacks).
    slow_callback_hook: OnceLock<Py<PyAny>>,
    /// asyncio's `loop.slow_callback_duration`, in nanoseconds. A fixed
    /// 100ms was baked in here, so the standard knob applications and
    /// test harnesses tune did nothing -- and the facade did not even
    /// carry the attribute, so reading it raised AttributeError.
    slow_callback_ns: AtomicU64,
    /// The `cadeloop.Loop` facade that owns this core, so the native
    /// `create_task` / `create_future` fast paths can pass `loop=` without
    /// a round trip through Python. Strong, like the hooks above -- the
    /// facade and its core have the same lifetime.
    owner: OnceLock<Py<PyAny>>,
    high_water: usize,
    low_water: usize,
    /// R-038 TCP Fast Open on listeners this loop creates.
    tfo: bool,
    /// R-038 SIO_LOOPBACK_FAST_PATH on connected/accepted sockets
    /// (Windows only; a no-op elsewhere).
    loopback_fast_path: bool,
}

impl CoreLoop {
    fn check_closed(&self) -> PyResult<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(PyRuntimeError::new_err("Event loop is closed"))
        } else {
            Ok(())
        }
    }

    /// asyncio's debug-mode `_check_thread` for call_soon/call_later/
    /// call_at. Outside debug mode those calls are accepted from any
    /// thread (gil_boundary), as asyncio accepts them.
    #[inline]
    fn check_thread(&self) -> PyResult<()> {
        if self.debug.load(Ordering::Relaxed) && self.state.owned_by_other_thread() {
            return Err(PyRuntimeError::new_err(
                "Non-thread-safe operation invoked on an event loop other than the current one",
            ));
        }
        Ok(())
    }

    pub(crate) fn require_owner(&self) -> PyResult<&Py<PyAny>> {
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

    /// A fresh loop-clock reading (core::time module docs): one vDSO
    /// clock read, safe from any thread.
    #[inline]
    fn fresh_ns(&self) -> Ticks {
        raw_now().saturating_add_signed(self.clock_offset)
    }

    /// Exclusive access to (net, reactor) — see gil_boundary contract.
    pub(crate) fn with_net<R>(
        &self,
        f: impl FnOnce(&mut NetState, &mut Reactor<HandleRef>) -> R,
    ) -> PyResult<R> {
        self.state.with(|st| f(&mut st.net, &mut st.reactor))
    }

    /// [`CoreLoop::with_net`] for read-only accessors that have an answer
    /// of their own when the state is out of reach -- another thread while
    /// the loop is parked in its poll, or a re-entered borrow
    /// (gil_boundary): None there, `f` not run, instead of a RuntimeError.
    pub(crate) fn try_with_net<R>(
        &self,
        f: impl FnOnce(&mut NetState, &mut Reactor<HandleRef>) -> R,
    ) -> Option<R> {
        self.state.try_with(|st| f(&mut st.net, &mut st.reactor)).ok()
    }

    pub(crate) fn water_marks(&self) -> (usize, usize) {
        (self.high_water, self.low_water)
    }

    /// Drop graveyarded Python refs / buffers outside the state cell.
    pub(crate) fn drain_graveyards(&self, py: Python<'_>) -> PyResult<()> {
        let (entries, bufs, pys, protos, timers, socks, handles) = self.state.with(|st| {
            (
                std::mem::take(&mut st.net.graveyard_entries),
                std::mem::take(&mut st.net.graveyard_bufs),
                std::mem::take(&mut st.net.graveyard_py),
                std::mem::take(&mut st.net.graveyard_protos),
                st.reactor.take_graveyard(),
                std::mem::take(&mut st.net.graveyard_sockets),
                std::mem::take(&mut st.net.graveyard_handles),
            )
        })?;
        // Watch handles the loop no longer owns -- removed, or displaced by
        // a newer add_reader/add_writer on the fd -- are CANCELLED, as
        // asyncio's remove_reader and _add_reader cancel them: a copy may
        // still sit in this tick's batch, and code like sock_recv's
        // cleanup tells "my watch" from "a newer call's watch" by
        // `handle.cancelled()`. Then they give up their references
        // (handles.rs module docs).
        for h in &handles {
            let h = h.get();
            h.do_cancel();
            h.release(py);
        }
        // Sockets handed out by `get_extra_info("socket")` are CLOSED, not
        // merely dropped. Each holds its own duplicate of the connection,
        // and a duplicate is another owner: if the application kept the
        // object, dropping our reference leaves the peer without an EOF
        // and the connection allocated until that object happens to be
        // collected. Closing is what makes the transport's close final.
        for sock in &socks {
            let _ = sock.call_method0(py, intern!(py, "close"));
        }
        drop((entries, bufs, pys, protos, timers, socks, handles));
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
            Err(e) => self.report_net_error(
                py,
                "Exception in network protocol callback",
                e.into_value(py).into_any(),
            ),
        }
    }

    /// The two hooks below route errors to the facade's
    /// call_exception_handler, which deliberately RE-RAISES
    /// KeyboardInterrupt/SystemExit from a custom handler (CPython
    /// parity). Those two must keep propagating here -- demoting them to
    /// an unraisable warning kept the loop running through a Ctrl-C the
    /// user's own handler chose to surface. Anything else the hook call
    /// raises stays non-fatal (the facade already fell back to the
    /// default handler internally before letting it escape).
    fn hook_error_verdict(&self, py: Python<'_>, e: PyErr, ctx: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
        if e.is_instance_of::<PyKeyboardInterrupt>(py) || e.is_instance_of::<PySystemExit>(py) {
            return Err(e);
        }
        e.write_unraisable(py, ctx);
        Ok(())
    }

    pub(crate) fn report_net_error(&self, py: Python<'_>, message: &str, exc: Py<PyAny>) -> PyResult<()> {
        match self.net_error_hook.get() {
            Some(hook) => match hook.call1(py, (message, &exc)) {
                Ok(_) => Ok(()),
                Err(e) => self.hook_error_verdict(py, e, None),
            },
            None => {
                let err = PyRuntimeError::new_err(message.to_string());
                err.write_unraisable(py, Some(exc.bind(py)));
                Ok(())
            }
        }
    }

    /// `report_net_error` for a transport's own failure, with the
    /// `transport` and `protocol` context keys asyncio's `_fatal_error`
    /// and flow-control reports carry.
    pub(crate) fn report_transport_error(
        &self,
        py: Python<'_>,
        message: &str,
        exc: Py<PyAny>,
        transport: Option<Py<PyAny>>,
        protocol: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        match self.net_error_hook.get() {
            Some(hook) => match hook.call1(py, (message, &exc, transport, protocol)) {
                Ok(_) => Ok(()),
                Err(e) => self.hook_error_verdict(py, e, None),
            },
            None => self.report_net_error(py, message, exc),
        }
    }

    fn report_failure(&self, py: Python<'_>, handle: &Bound<'_, PyAny>, err: PyErr) -> PyResult<()> {
        match self.error_hook.get() {
            Some(hook) => match hook.call1(py, (handle, err.value(py))) {
                Ok(_) => Ok(()),
                Err(hook_err) => self.hook_error_verdict(py, hook_err, Some(handle)),
            },
            None => {
                err.write_unraisable(py, Some(handle));
                Ok(())
            }
        }
    }

    /// Build a Handle (created OUTSIDE the cell: allocation can GC).
    pub(crate) fn make_handle(
        &self,
        py: Python<'_>,
        callback: &Bound<'_, PyAny>,
        args: CallArgs,
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
        let context: (Py<PyAny>, bool) = match context {
            Some(c) if !c.is_none() => (c.clone().unbind(), false),
            _ => self.snaps.capture(py)?,
        };
        let debug = self.debug.load(Ordering::Relaxed);
        // asyncio's debug-mode `_source_traceback`. No frame is pushed for
        // this native call, so extract_stack()'s default -- its caller's
        // caller -- is the Python code that called call_soon & co, exactly
        // the frame asyncio trims its own scheduling frames down to.
        let source_tb = if debug {
            Some(
                py.import(intern!(py, "asyncio.format_helpers"))?
                    .call_method0(intern!(py, "extract_stack"))?
                    .unbind(),
            )
        } else {
            None
        };
        Handle::new(py, callback.clone().unbind(), args, context, debug, source_tb)
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
    ///
    /// `batch` is the dispatch buffer, owned by `run_forever`'s frame and
    /// empty between ticks. It used to live in a `thread_local!`, which in
    /// a cdylib is a `__tls_get_addr` call on every tick.
    fn tick(
        &self,
        slf: &Bound<'_, CoreLoop>,
        py: Python<'_>,
        batch: &mut VecDeque<HandleRef>,
        scratch: &mut Scratch,
    ) -> PyResult<()> {
        let stopping = self.stopping.load(Ordering::Acquire);
        debug_assert!(batch.is_empty());
        type TickOut = (std::io::Result<()>, bool, Vec<NetEvent>, Option<Graveyards>, bool);
        let (poll_result, parked, events, graveyard, special): TickOut = self.state.with(|st| {
            // Phase 1: flush corked writes from last tick's callbacks.
            if !st.net.flush_list.is_empty() {
                let list = std::mem::take(&mut st.net.flush_list);
                for tid in list {
                    net::flush_pending(py, &mut st.net, st.reactor.backend_mut(), tid);
                }
            }
            st.reactor.prepare_tick();
            let timeout = if stopping || !st.net.events.is_empty() {
                Duration::ZERO
            } else if st.net.any_starved_listener {
                // R-032: a starved listener or datagram endpoint has
                // NO operation outstanding -- that is what starved
                // means -- so nothing will ever complete to wake this
                // park. Without a bound the loop sleeps until some
                // unrelated event arrives, and on an idle server that
                // is never: the resource stays deaf even after
                // whatever exhausted the system has recovered.
                st.reactor.poll_timeout().min(net::starved_retry_in(&st.net, st.reactor.time_cached()))
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
                return (poll_result, parked, Vec::new(), None, false);
            }
            // Phase 2: translate completions (no Python execution).
            st.reactor.finish_poll_after(parked);
            let mut comps = std::mem::take(&mut st.completions_scratch);
            st.reactor.drain_completions(&mut comps);
            if !comps.is_empty() {
                st.net.now_ns = st.reactor.time_cached();
                net::translate(py, &mut st.net, st.reactor.backend_mut(), &comps);
                comps.clear();
            }
            // A listener whose accept pool ran dry on a transient
            // failure has no completion left to retry it (R-032) --
            // which is exactly why this cannot live inside the
            // completions branch. On an otherwise idle loop no
            // completion ever arrives, so gating the retry on one left
            // the listener deaf for good. Cheap when nothing is
            // starved: one bool load and return.
            if st.net.any_starved_listener {
                let now = st.reactor.time_cached();
                st.net.now_ns = now; // the re-post's own failure path reads it
                net::retry_starved_listeners(&mut st.net, st.reactor.backend_mut(), now);
            }
            st.completions_scratch = comps;
            // Readiness watch callbacks join the ready queue AND this tick's
            // batch, as asyncio runs an iteration's I/O callbacks in that
            // same iteration (see `absorb_into_batch`).
            if !st.net.ready_scratch.is_empty() {
                for h in st.net.ready_scratch.drain(..) {
                    st.reactor.push_ready(HandleRef(h));
                }
                st.reactor.absorb_into_batch();
            }
            let events = std::mem::take(&mut st.net.events);
            // The dispatch batch, in the SAME cell entry: every callback
            // ready when the tick began, plus this tick's timers and
            // readiness watches (batch_left was snapshotted for exactly
            // those). It runs BEFORE the transport events below, as
            // asyncio's _run_once appends an iteration's I/O callbacks
            // behind the callbacks already in `_ready`.
            st.reactor.take_batch(batch);
            let graveyard = if st.net.has_graveyards() || st.reactor.has_graveyard() {
                Some(take_graveyards(st))
            } else {
                None
            };
            (Ok(()), parked, events, graveyard, st.net.has_special_graveyards())
        })?;
        drop(graveyard);
        poll_result?;
        if special && events.is_empty() {
            // The event path drains these itself after dispatch.
            self.drain_graveyards(py)?;
        }

        // Ctrl-C responsiveness: a parked poll returns promptly on
        // EINTR, so check right after it; busy ticks check every 64th
        // tick instead of paying the call per tick. The batch may
        // already be filled — hand tokens back (front, reverse order)
        // so an interrupt loses nothing and preserves FIFO.
        let tick_no = self.tick_no.load(Ordering::Relaxed);
        self.tick_no.store(tick_no.wrapping_add(1), Ordering::Relaxed);
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
                // The transport events too: dropped here, they took their
                // payloads and teardowns with them -- and a caller that
                // runs the loop again after a KeyboardInterrupt (asyncio.run
                // cancelling the remaining tasks does) never saw that data
                // or those connection_lost calls.
                self.requeue_events(events)?;
                return Err(err);
            }
        }

        // Phase 3: ready-callback batch (R-054). A fatal error
        // (KeyboardInterrupt/SystemExit from a callback) returns the
        // undispatched tail to the queue front -- and this tick's transport
        // events, which have not run yet, to theirs -- so nothing is lost
        // and FIFO order holds across the unwind.
        let debug = self.debug.load(Ordering::Relaxed);
        while let Some(token) = batch.pop_front() {
            let started = debug.then(std::time::Instant::now);
            let fatal = match run_handle(py, token.get(), scratch) {
                Ok(DispatchOutcome::Done) => None,
                // A fatal raise from the hook unwinds exactly like one from
                // the callback itself.
                Ok(DispatchOutcome::Failed(err)) => {
                    self.report_failure(py, token.0.bind(py).as_any(), err).err()
                }
                Err(e) => Some(e),
            };
            if let Some(err) = fatal {
                // This callback ran (and raised); everything still in the
                // batch goes back to the queue front, in order.
                if !batch.is_empty() {
                    self.state.with(|st| {
                        for t in batch.drain(..).rev() {
                            st.reactor.unpop_ready(t);
                        }
                    })?;
                }
                release_token(py, token);
                self.requeue_events(events)?;
                return Err(err);
            }
            if let Some(started) = started {
                let elapsed = started.elapsed();
                if elapsed.as_nanos() as u64 > self.slow_callback_ns.load(Ordering::Relaxed) {
                    if let Some(hook) = self.slow_callback_hook.get() {
                        if let Err(e) = hook.call1(py, (&token.0, elapsed.as_secs_f64())) {
                            e.write_unraisable(py, Some(token.0.bind(py).as_any()));
                        }
                    }
                }
            }
            // Drop what the callback held NOW, not when the batch ends. A
            // batch is the whole ready snapshot -- thousands of callbacks
            // when a gather fans out -- and holding every dispatched
            // callback's task, future and context until its end kept them
            // alive through the young collections they would have died
            // before: CPython 3.11 counts a late free against the next
            // gen-0 trigger and promotes the survivors, so the recursive
            // task_fib ran 16% more young and 33% more full collections
            // (-24% throughput) until this moved here. The handle itself
            // goes too: popping each token, rather than iterating the batch
            // and draining it afterwards, is also the cheaper walk for the
            // one-callback ticks a coroutine chain runs on. (After the hooks
            // above: the slow-callback report formats the callback.)
            release_token(py, token);
        }
        // Let the shared context snapshot go once nothing pending uses it.
        self.snaps.trim(py);

        // Phases 4-6 only exist when transport events fired: dispatch the
        // protocol callbacks, flush the writes this tick corked (same tick,
        // R-035), then dispatch the teardowns that flush produced. Anything
        // they schedule runs next tick, as in asyncio.
        if !events.is_empty() {
            net::dispatch_events(py, slf, events)?;
            let events = self.flush_corked(py)?;
            self.drain_graveyards(py)?;
            if !events.is_empty() {
                net::dispatch_events(py, slf, events)?;
            }
        }
        Ok(())
    }

    /// Put transport events this tick took but did not dispatch back at
    /// the FRONT of the queue (ahead of any a callback produced since), so
    /// the next tick delivers them in their original order.
    fn requeue_events(&self, events: Vec<NetEvent>) -> PyResult<()> {
        if events.is_empty() {
            return Ok(());
        }
        self.with_net(|net, _| {
            let mut later = std::mem::take(&mut net.events);
            net.events = events;
            net.events.append(&mut later);
        })
    }
}

/// Drop every handle in a dispatched batch.
///
/// Each one-shot handle already gave up its callback, arguments and
/// context right after it ran (handles.rs module docs; readiness watches
/// keep theirs); the release repeated here is a no-op for those and
/// covers the one a fatal error cut off before its own. The drops are
/// straight `Py_DECREF`s: we are on the loop thread inside `run_forever`,
/// attached by construction, so `Py::drop`'s per-object thread-local
/// "am I attached?" probe (a `__tls_get_addr` call in a cdylib) buys
/// nothing here.
#[inline]
fn release_batch(py: Python<'_>, batch: &mut VecDeque<HandleRef>) {
    for token in batch.drain(..) {
        release_token(py, token);
    }
}

/// [`release_batch`] for one token.
#[inline]
fn release_token(py: Python<'_>, token: HandleRef) {
    token.get().release_after_dispatch(py);
    // SAFETY: attached (see `release_batch`); into_ptr transfers our reference.
    unsafe { ffi::Py_DECREF(token.into_ptr()) };
}

/// What the tick moves out of the cell to drop. `graveyard_sockets` and
/// `graveyard_handles` are not here: both need work beyond a drop (an
/// explicit close, a reference release), which `drain_graveyards` does.
type Graveyards = (
    Vec<crate::net::TransportEntry>,
    Vec<crate::net::WriteBuf>,
    Vec<Py<PyAny>>,
    Vec<crate::net::ProtoKind>,
    Vec<HandleRef>,
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
                        rio_cq_size=65536, rio_rq_recv=32, rio_rq_send=32,
                        tfo=false, loopback_fast_path=true))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        backend: &str,
        spin_us: u64,
        high_water: usize,
        low_water: usize,
        rio_cq_size: u32,
        rio_rq_recv: u32,
        rio_rq_send: u32,
        tfo: bool,
        loopback_fast_path: bool,
    ) -> PyResult<Self> {
        let kind = BackendKind::parse(backend).ok_or_else(|| {
            PyValueError::new_err(format!(
                "invalid backend {backend:?}: expected 'auto', 'iocp', 'rio' or 'epoll'"
            ))
        })?;
        // Align the loop clock with time.monotonic() where the raw platform
        // clock is not already the same one (core::time module docs).
        let clock_offset = if RAW_IS_PYTHON_MONOTONIC {
            0
        } else {
            let py_now: i64 =
                py.import(intern!(py, "time"))?.call_method0(intern!(py, "monotonic_ns"))?.extract()?;
            py_now.saturating_sub(raw_now() as i64)
        };
        let cfg = ReactorConfig {
            backend: kind,
            spin_us,
            clock_offset_ns: clock_offset,
            backend_opts: cadeloop_core::backend::BackendOptions { rio_cq_size, rio_rq_recv, rio_rq_send },
            ..Default::default()
        };
        let reactor: Reactor<HandleRef> = Reactor::new(cfg)?;
        let (xqueue, waker) = reactor.cross_thread_handles();
        let timer_cancels = reactor.timer_cancel_counter();
        Ok(CoreLoop {
            state: StateCell::new(LoopState {
                reactor,
                net: NetState::default(),
                completions_scratch: Vec::with_capacity(512),
            }),
            xqueue,
            snaps: SnapCache::default(),
            waker,
            timer_cancels,
            closed: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            debug: AtomicBool::new(false),
            clock_offset,
            tick_no: AtomicU64::new(0),
            error_hook: OnceLock::new(),
            net_error_hook: OnceLock::new(),
            slow_callback_hook: OnceLock::new(),
            slow_callback_ns: AtomicU64::new(100_000_000),
            owner: OnceLock::new(),
            high_water,
            low_water,
            tfo,
            loopback_fast_path,
        })
    }

    // ---- lifecycle -------------------------------------------------------

    fn run_forever(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let this = slf.get();
        this.check_closed()?;
        let guard = this.state.claim()?;
        // The reactor's spare dispatch buffer, lent for this run: it and the
        // ready queue trade places every tick (Reactor::take_batch).
        let mut batch: VecDeque<HandleRef> = this.state.with(|st| st.reactor.take_spare())?;
        let mut scratch = Scratch::default();
        let result = loop {
            match this.tick(slf, py, &mut batch, &mut scratch) {
                Err(e) => break Err(e),
                Ok(()) => {
                    if this.stopping.load(Ordering::Acquire) {
                        break Ok(());
                    }
                }
            }
        };
        this.stopping.store(false, Ordering::Release);
        // Every tick, its unwind paths included, leaves the batch empty; a
        // straggler is released here, outside the cell, so the buffer goes
        // back empty.
        if !batch.is_empty() {
            release_batch(py, &mut batch);
        }
        this.state.with(|st| st.reactor.put_spare(batch))?;
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
        self.snaps.clear(py);
        // R-122 "close with pending ops": tear down every live connection
        // and listener (cancels in-flight kernel ops), then drop all
        // pending work outside the cell.
        let (handles, py_refs, net_events) = self.state.with(|st| {
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
            let dropped_py: Vec<Py<PyAny>> =
                net::cancel_standalone_ops(&mut st.net, st.reactor.backend_mut());
            let mut dropped: Vec<HandleRef> = Vec::new();
            // R-073 reap guards: the sweeps above could not free the
            // buffers of the ops they cancelled, because the kernel may
            // still own them until each op's completion is dequeued -- and
            // a closed loop never polls again. Pump the backend here so
            // those completions are reaped now instead of the buffers
            // (queued response bodies, up to high_water per connection)
            // living as long as anything still references the dead loop.
            net::reap_at_close(py, &mut st.net, st.reactor.backend_mut());
            dropped.extend(st.reactor.clear_pending());
            dropped.extend(std::mem::take(&mut st.net.ready_scratch).into_iter().map(HandleRef));
            // The backend's side of each watch goes with the callback. On
            // IOCP a watch is a request the kernel owns until it is
            // cancelled and reaped; left armed, it outlived the loop and
            // completed into freed memory once the socket saw data.
            for (fd, h) in st.net.readers.drain() {
                let _ = st.reactor.backend_mut().set_watch(fd, false, false);
                dropped.push(HandleRef(h));
            }
            for (fd, h) in st.net.writers.drain() {
                let _ = st.reactor.backend_mut().set_watch(fd, false, false);
                dropped.push(HandleRef(h));
            }
            // Events that will never be dispatched (a fatal error unwound
            // the batch they were in) may carry the only reference to a
            // socket: an accepted connection nobody wired, a connect whose
            // future nobody resolved. Dropping the event dropped a raw
            // descriptor -- an open connection the peer never saw close.
            for ev in &st.net.events {
                match *ev {
                    NetEvent::Accepted { sock, .. } => netsys::close(sock),
                    NetEvent::ConnectDone { sock, .. } => {
                        st.reactor.backend_mut().detach_socket(sock);
                        netsys::close(sock);
                    }
                    _ => {}
                }
            }
            // Last: everything above may still need the poller (unwatching
            // is an epoll_ctl), and nothing after this point polls.
            st.reactor.backend_mut().release_poller();
            // ADR-5: NetEvent variants OWN Python references. Clearing the
            // vector here would decref them while `with` holds the exclusive
            // &mut, and a finalizer reached by one of those decrefs can
            // re-enter the loop and alias the state. Move them out and drop
            // after leaving the cell, exactly as the tick path does.
            (dropped, dropped_py, std::mem::take(&mut st.net.events))
        })?;
        for h in &handles {
            h.get().release(py);
        }
        drop(handles);
        drop(py_refs);
        drop(net_events);
        self.drain_graveyards(py)?;
        Ok(())
    }

    // ---- time (R-061) ----------------------------------------------------

    fn time(&self) -> f64 {
        ticks_to_secs_f64(self.fresh_ns())
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
        self.check_thread()?;
        let handle =
            self.make_handle(py, &callback, CallArgs::from_tuple(&args), context.as_ref(), "call_soon")?;
        self.enqueue_soon(py, handle)
    }

    /// Thread-safe variant (R-022): see `enqueue_soon` for when it takes
    /// the lock-free queue and a wakeup.
    #[pyo3(signature = (callback, *args, context=None))]
    fn call_soon_threadsafe(
        &self,
        py: Python<'_>,
        callback: Bound<'_, PyAny>,
        args: Bound<'_, PyTuple>,
        context: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<Handle>> {
        self.check_closed()?;
        let handle = self.make_handle(
            py,
            &callback,
            CallArgs::from_tuple(&args),
            context.as_ref(),
            "call_soon_threadsafe",
        )?;
        self.enqueue_soon(py, handle)
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
        let this = self;
        this.check_closed()?;
        this.check_thread()?;
        let (deadline, when) = this.later_deadline(delay);
        let base =
            // "call_at": asyncio's call_later is call_at(time() + delay),
            // so its debug-mode callback errors name call_at().
            this.make_handle(py, &callback, CallArgs::from_tuple(&args), context.as_ref(), "call_at")?;
        this.schedule_timer(py, deadline, when, base)
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
        let this = self;
        this.check_closed()?;
        this.check_thread()?;
        let base =
            this.make_handle(py, &callback, CallArgs::from_tuple(&args), context.as_ref(), "call_at")?;
        this.schedule_timer(py, secs_f64_to_ticks(when), when, base)
    }

    /// The scheduling fast paths as builtin functions bound to this core:
    /// `(call_soon, call_soon_threadsafe, call_later, call_at)`.
    ///
    /// Same names, signatures and semantics as the methods above, but
    /// entered through a raw METH_FASTCALL function that reads its
    /// arguments straight off the vectorcall array: no generic argument
    /// extraction, and no `*args` tuple built per call only to be unpacked
    /// again at dispatch. The facade binds these onto the loop instance,
    /// which is where asyncio's C Task and Future look `call_soon` up on
    /// every step and every done-callback.
    fn fast_methods<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyTuple>> {
        let py = slf.py();
        let mut out = Vec::with_capacity(FAST_DEFS.0.len());
        for def in FAST_DEFS.0.iter() {
            // SAFETY: `def` lives in a static (stable address, never
            // freed); PyCFunction_NewEx takes a new reference to `slf`.
            let f = unsafe {
                ffi::PyCFunction_NewEx(std::ptr::from_ref(def).cast_mut(), slf.as_ptr(), std::ptr::null_mut())
            };
            if f.is_null() {
                return Err(PyErr::fetch(py));
            }
            out.push(unsafe { Bound::from_owned_ptr(py, f) });
        }
        PyTuple::new(py, out)
    }

    // ---- TCP (M1) --------------------------------------------------------

    /// Bind + listen (+ start the accept pool unless `start=false`).
    /// Returns (listener id, (ip, port) bound name, raw fd).
    ///
    /// `skip_unavailable`: return None instead of raising when no socket
    /// of the address's family can be created at all -- create_server's
    /// rule for a multi-family resolution, where a host without IPv6
    /// still serves on IPv4.
    #[pyo3(signature = (ip, port, factory, backlog=1024, reuse_addr=true, reuse_port=false,
                        accept_pool=64, start=true, flowinfo=0, scope_id=0, skip_unavailable=false))]
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
        flowinfo: u32,
        scope_id: u32,
        skip_unavailable: bool,
    ) -> PyResult<Option<(u64, Py<PyAny>, u64)>> {
        self.check_closed()?;
        let spec = ListenSpec { backlog, reuse_addr, reuse_port, tfo: self.tfo, flowinfo, scope_id };
        let sock = match bind_listen_socket(ip, port, &spec) {
            Ok(sock) => sock,
            Err(ListenError::Socket(_)) if skip_unavailable => return Ok(None),
            Err(e) => return Err(e.into_pyerr(py, ip, port, &spec)),
        };
        self.listen_socket(py, sock, net::ListenerKind::Factory(factory.unbind()), accept_pool, start, None)
            .map(Some)
    }

    /// Bind + listen for the native HTTP/ASGI engine (M2, R-080). Every
    /// accepted connection is parsed and answered natively; `pyloop` is
    /// the facade loop (receive() waiters / non-eager tasks need it).
    #[pyo3(signature = (ip, port, app, pyloop, state=None, backlog=1024, reuse_addr=true,
                        reuse_port=false, accept_pool=64, eager=true, max_header_bytes=65536,
                        max_headers=100, max_url=8192, max_body=None,
                        request_line_timeout=5.0, keepalive_idle=75.0, tls=None,
                        flowinfo=0, scope_id=0))]
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
        flowinfo: u32,
        scope_id: u32,
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
        let spec = ListenSpec { backlog, reuse_addr, reuse_port, tfo: self.tfo, flowinfo, scope_id };
        let sock = bind_listen_socket(ip, port, &spec).map_err(|e| e.into_pyerr(py, ip, port, &spec))?;
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
        self.listen_socket(py, sock, kind, accept_pool, true, None)
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
        self.listen_socket(py, fd as RawSocket, kind, accept_pool, true, None)
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
    /// `owner`: the socket object whose descriptor `fd` is; it stays the
    /// owner and is closed after connection_lost (see udp_wire).
    #[pyo3(signature = (fd, datagram_received, error_received, connection_lost, owner=None))]
    fn udp_open(
        &self,
        py: Python<'_>,
        fd: u64,
        datagram_received: Bound<'_, PyAny>,
        error_received: Bound<'_, PyAny>,
        connection_lost: Bound<'_, PyAny>,
        owner: Option<Bound<'_, PyAny>>,
    ) -> PyResult<u64> {
        self.check_closed()?;
        let owner = owner.filter(|o| !o.is_none()).map(Bound::unbind);
        let wired = self.with_net(|net, reactor| {
            net::udp_wire(
                net,
                reactor.backend_mut(),
                fd as cadeloop_core::backend::RawSocket,
                datagram_received.clone().unbind(),
                error_received.clone().unbind(),
                connection_lost.clone().unbind(),
                owner,
            )
        })?;
        self.drain_graveyards(py)?;
        wired.map_err(net::io_error)
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
    ///
    /// Accepts both address forms the socket module uses: `(host, port)`
    /// and the IPv6 `(host, port, flowinfo, scope_id)`. The four-element
    /// form has to round-trip, because it is what `datagram_received`
    /// hands the application for an IPv6 peer -- and for a link-local one
    /// the scope id is what makes the reply routable.
    #[pyo3(signature = (did, data, addr=None))]
    fn udp_sendto(
        &self,
        py: Python<'_>,
        did: u64,
        data: &[u8],
        addr: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.check_closed()?;
        let addr = addr.filter(|a| !a.is_none()).map(parse_addr_tuple).transpose()?;
        self.with_net(|net, reactor| net::udp_sendto(py, net, reactor.backend_mut(), did, data, addr))?
            .map_err(net::io_error)?;
        self.drain_graveyards(py)
    }

    /// R-058 close/abort. close flushes queued sends; abort drops them.
    #[pyo3(signature = (did, abort=false))]
    fn udp_close(&self, py: Python<'_>, did: u64, abort: bool) -> PyResult<()> {
        self.with_net(|net, reactor| {
            net::udp_close(py, net, reactor.backend_mut(), did, abort);
        })?;
        self.drain_graveyards(py)?;
        net::schedule_pending_lost(py, self)
    }

    /// R-080 timeout sweep tick (armed as a coarse repeating timer by the
    /// facade server). Returns (head_timeouts, idle_closes) this pass.
    fn http_sweep(&self, py: Python<'_>) -> PyResult<(u32, u32)> {
        let (head, idle, cancel_drivers) = self.with_net(|net, reactor| {
            let now_ns = reactor.time_cached();
            let backend = reactor.backend_mut();
            net::http_sweep(py, net, backend, now_ns)
        })?;
        self.drain_graveyards(py)?;
        // Cancel the drivers of reclaimed disconnected connections OUTSIDE
        // the state cell (cancel() runs Python): unwinds each abandoned
        // coroutine so its AppTask + frame are released. These are already
        // included in the idle count reported to the facade.
        for driver in cancel_drivers {
            let _ = driver.bind(py).call_method0(intern!(py, "cancel"));
        }
        Ok((head, idle))
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
    ///
    /// With `owner` (the caller's socket object, whose descriptor `fd` is)
    /// the object stays the owner, as asyncio leaves it: it keeps working
    /// while the server runs, the listener's close closes it, and a
    /// failure here leaves it open. Without, the listener owns the
    /// (detached) descriptor from entry and every error path closes it
    /// exactly once -- the Python side must not, or a threaded process's
    /// second close lands on whatever reused the number.
    #[pyo3(signature = (fd, factory, accept_pool=64, start=true, owner=None))]
    fn listen_fd(
        &self,
        py: Python<'_>,
        fd: u64,
        factory: Bound<'_, PyAny>,
        accept_pool: usize,
        start: bool,
        owner: Option<Bound<'_, PyAny>>,
    ) -> PyResult<(u64, Py<PyAny>, u64)> {
        let owner = owner.filter(|o| !o.is_none()).map(Bound::unbind);
        if let Err(e) = self.check_closed() {
            if owner.is_none() {
                netsys::close(fd as RawSocket);
            }
            return Err(e);
        }
        self.listen_socket(
            py,
            fd as RawSocket,
            net::ListenerKind::Factory(factory.unbind()),
            accept_pool,
            start,
            owner,
        )
    }

    /// The live transport whose descriptor `fd` is, if any (asyncio's
    /// `_ensure_fd_no_transport`, for add_reader and friends).
    fn transport_for_fd(&self, py: Python<'_>, fd: u64) -> PyResult<Option<Py<PyAny>>> {
        let found = self.with_net(|net, _| net.transport_for_fd(py, fd as RawSocket))?;
        Ok(match found {
            Some((false, transport)) => Some(transport),
            Some((true, connection_lost)) => connection_lost.getattr(py, "__self__").ok(),
            None => None,
        })
    }

    /// Give a listener its asyncio Server facade, so every connection it
    /// accepts is counted (`_attach`/`_detach`) for `Server.wait_closed()`.
    fn listener_set_server(&self, lid: u64, server: Bound<'_, PyAny>) -> PyResult<()> {
        let old = self.with_net(|net, _| net::listener_set_server(net, lid, server.unbind()))?;
        drop(old); // outside the cell
        Ok(())
    }

    /// R-052: the facade marks the signal-wakeup pipe's read end here so
    /// poll elision does not treat its permanent watch as I/O interest.
    #[pyo3(signature = (fd=None))]
    fn set_signal_wakeup_fd(&self, fd: Option<u64>) -> PyResult<()> {
        self.with_net(|_net, reactor| reactor.set_wake_only(fd.map(|f| f as RawSocket)))
    }

    /// Start (or restart) accepting on a listener created with start=false.
    fn listener_start(&self, lid: u64) -> PyResult<()> {
        let started = self.with_net(|net, reactor| {
            net.now_ns = reactor.now_fresh(); // a failed first post backs off from now
            net::listener_start(net, reactor.backend_mut(), lid)
        })?;
        started.map_err(net::io_error)
    }

    fn listener_close(&self, py: Python<'_>, lid: u64) -> PyResult<()> {
        self.with_net(|net, reactor| net::listener_teardown(net, reactor.backend_mut(), lid))?;
        self.drain_graveyards(py)
    }

    /// Graceful shutdown, phase 1 (R-092). Call after the listener is
    /// closed and before `close()`: ends keep-alive on every live HTTP
    /// connection, closes the idle ones, starts the WebSocket closing
    /// handshake, and returns how many connections still have a request
    /// in flight. The caller keeps ticking the loop until that reaches
    /// zero or its grace deadline expires.
    fn http_begin_shutdown(&self, py: Python<'_>) -> PyResult<usize> {
        let busy = self.with_net(|net, reactor| net::http_begin_shutdown(py, net, reactor.backend_mut()))?;
        self.drain_graveyards(py)?;
        Ok(busy)
    }

    /// R-060 latency mode: when set, HTTP response bytes go on the wire
    /// as soon as they are wire-ready rather than being corked until the
    /// tick's flush phase. Trades syscalls for tail latency; `Config`
    /// derives it from `latency_mode` (on for `spin`).
    fn set_immediate_flush(&self, enabled: bool) -> PyResult<()> {
        self.with_net(|net, _| net.flush_immediately = enabled)
    }

    /// Live *native HTTP* connections -- the drain loop's convergence
    /// test. Deliberately not `transports.len()`: that also counts the
    /// Python-protocol transports an application opens for its own
    /// outbound work (a database client, an upstream HTTP call), and
    /// those need not close for the server to be drained. Counting them
    /// would make every such deployment wait out the whole grace period.
    fn http_connection_count(&self) -> PyResult<usize> {
        self.with_net(|net, _| net.http_conn_count())
    }

    /// Count of live connections spawned by any listener (facade
    /// wait_closed support is in Python; this is for stats).
    fn listener_count(&self) -> PyResult<usize> {
        self.with_net(|net, _| net.listeners.len())
    }

    /// Begin an async connect; `fut` resolves to the connected socket
    /// handle (u64) or an OSError.
    #[pyo3(signature = (addr, fut, local_addr=None))]
    /// Begin an async connect to a resolved address.
    ///
    /// Both addresses are the socket module's sockaddr tuples, not
    /// host/port pairs: `(host, port)` for IPv4 and
    /// `(host, port, flowinfo, scope_id)` for IPv6. Flattening the IPv6
    /// form loses the interface scope, and a link-local peer
    /// (`fe80::...%eth0`) is then unreachable however cleanly it
    /// resolved.
    fn tcp_connect(
        &self,
        _py: Python<'_>,
        addr: Bound<'_, PyAny>,
        fut: Bound<'_, PyAny>,
        local_addr: Option<Bound<'_, PyAny>>,
    ) -> PyResult<(u32, u32)> {
        self.check_closed()?;
        let sockaddr = parse_addr_tuple(addr)?;
        let family = if sockaddr.is_ipv4() { netsys::AF_INET } else { netsys::AF_INET6 };
        let sock = netsys::create_tcp(family).map_err(net::io_error)?;
        if self.loopback_fast_path {
            // Best-effort by design (R-038/R-131): it needs both ends to
            // opt in and is unsupported on some builds.
            let _ = netsys::set_loopback_fast_path(sock);
        }
        if let Some(local) = local_addr.filter(|a| !a.is_none()) {
            let laddr = match parse_addr_tuple(local.clone()) {
                Ok(a) => a,
                Err(e) => {
                    netsys::close(sock);
                    return Err(e);
                }
            };
            let lsa = netsys::build_sockaddr(laddr);
            if let Err(e) = netsys::bind(sock, &lsa) {
                netsys::close(sock);
                // Worded as create_connection words it, naming the local
                // address: the caller may have offered several.
                let Some(code) = e.raw_os_error() else { return Err(e.into()) };
                let strerror = net::os_strerror(code).to_lowercase();
                return Err(net::os_error_msg(
                    code,
                    format!("error while attempting to bind on address {}: {strerror}", local.repr()?),
                ));
            }
        }
        let fut: Py<PyAny> = fut.unbind();
        let sa = netsys::build_sockaddr(sockaddr);
        let res = self.with_net(|net, reactor| -> std::io::Result<(u32, u32)> {
            let backend = reactor.backend_mut();
            backend.register_socket(sock)?;
            match backend.post_connect(sock, &sa.buf[..sa.len]) {
                Ok(op) => {
                    net.ops.insert(op, net::OpTarget::Connect { fut, sock });
                    Ok((op.index, op.generation))
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
        match res {
            Err(e) => {
                netsys::close(sock);
                Err(net::io_error(e))
            }
            Ok(op) => Ok(op),
        }
    }

    /// Cancel an in-flight connect posted by `tcp_connect`.
    ///
    /// Without this, a caller whose `await` is cancelled -- which is
    /// every losing Happy Eyeballs attempt, not just an unusual one --
    /// left the operation running with its socket open until the OS gave
    /// up on the connect, which can be minutes. The completion still
    /// arrives (ABORTED) and `ConnectDone` closes the socket then, so
    /// this only ever brings that forward; the op mapping deliberately
    /// stays until the completion is dequeued (ADR-25).
    ///
    /// A stale or already-completed op is not an error: the race between
    /// cancelling and completing is normal and both outcomes are fine.
    fn cancel_connect(&self, op: (u32, u32)) -> PyResult<()> {
        let op = cadeloop_core::opslab::OpId { index: op.0, generation: op.1 };
        self.with_net(|_, reactor| {
            let _ = reactor.backend_mut().cancel(op);
        })
    }

    /// Close a connected socket that never reached `attach_stream`.
    ///
    /// The facade uses this when `protocol_factory()` (or building the
    /// SSLContext) raises between a successful connect and the transport
    /// that would have owned the descriptor: at that moment nothing else
    /// holds it, so the fd would leak, one per failure.
    ///
    /// It detaches before closing (ADR-25). The socket was registered
    /// with the backend by `tcp_connect`; closing it while the backend
    /// still lists it as associated leaves an entry keyed by a handle
    /// value the OS will hand out again, and the next socket to reuse
    /// that value is treated as already registered -- so its completions
    /// are never queued and it hangs silently.
    fn discard_socket(&self, sock: u64) -> PyResult<()> {
        self.with_net(|_, reactor| reactor.backend_mut().detach_socket(sock as RawSocket))?;
        netsys::close(sock as RawSocket);
        Ok(())
    }

    /// Wire a connected socket (from tcp_connect's future) into a transport.
    ///
    /// `owner`: the caller's socket object, when `sock` is its descriptor
    /// (`create_connection(sock=...)` and friends) -- see
    /// `TransportEntry::owner`.
    #[pyo3(signature = (sock, protocol, owner=None))]
    fn attach_stream(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        sock: u64,
        protocol: Bound<'_, PyAny>,
        owner: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<Transport>> {
        slf.get().check_closed()?;
        let owner = owner.filter(|o| !o.is_none()).map(Bound::unbind);
        net::wire_stream(py, slf, sock as RawSocket, protocol, owner, None)
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
        self.with_net(|_, reactor| reactor.backend_mut().register_pipe(handle as RawSocket))?
            .map_err(net::io_error)?;
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
            return Err(net::io_error(e));
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
        res.map_err(net::io_error)
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
        let handle = self
            .make_handle(py, &callback, CallArgs::from_tuple(&args), None, "add_reader")?
            .persistent(py)?;
        let handle = Py::new(py, handle)?;
        let token: Py<Handle> = handle.clone_ref(py);
        let sock = fd as RawSocket;
        self.with_net(|net, reactor| -> std::io::Result<()> {
            let displaced = net.readers.insert(sock, token);
            let w = net.writers.contains_key(&sock);
            if let Err(e) = reactor.backend_mut().set_watch(sock, true, w) {
                // Undo the map change. Leaving it makes the loop claim a
                // watcher the backend refused to register -- `remove_reader`
                // returns True for an fd nothing is watching -- and silently
                // discards the callback this call displaced, which was a
                // working watcher until a moment ago.
                let ours = match displaced {
                    Some(prev) => net.readers.insert(sock, prev),
                    None => net.readers.remove(&sock),
                };
                if let Some(h) = ours {
                    net.graveyard_handles.push(h);
                }
                return Err(e);
            }
            if let Some(old) = displaced {
                net.graveyard_handles.push(old);
            }
            Ok(())
        })?
        .map_err(net::io_error)?;
        self.drain_graveyards(py)?;
        Ok(handle)
    }

    fn remove_reader(&self, py: Python<'_>, fd: u64) -> PyResult<bool> {
        let sock = fd as RawSocket;
        let removed = self
            .with_net(|net, reactor| -> std::io::Result<bool> {
                let removed = net.readers.remove(&sock);
                let had = removed.is_some();
                let w = net.writers.contains_key(&sock);
                if had || w {
                    if let Err(e) = reactor.backend_mut().set_watch(sock, false, w) {
                        // Put it back: the backend is still watching, so the
                        // callback must stay reachable or its next readiness
                        // event finds no handler.
                        if let Some(h) = removed {
                            if let Some(newer) = net.readers.insert(sock, h) {
                                net.graveyard_handles.push(newer);
                            }
                        }
                        return Err(e);
                    }
                }
                if let Some(h) = removed {
                    net.graveyard_handles.push(h);
                }
                Ok(had)
            })?
            .map_err(net::io_error)?;
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
        let handle = self
            .make_handle(py, &callback, CallArgs::from_tuple(&args), None, "add_writer")?
            .persistent(py)?;
        let handle = Py::new(py, handle)?;
        let token: Py<Handle> = handle.clone_ref(py);
        let sock = fd as RawSocket;
        self.with_net(|net, reactor| -> std::io::Result<()> {
            let displaced = net.writers.insert(sock, token);
            let r = net.readers.contains_key(&sock);
            if let Err(e) = reactor.backend_mut().set_watch(sock, r, true) {
                // Same rollback as add_reader: see the note there.
                let ours = match displaced {
                    Some(prev) => net.writers.insert(sock, prev),
                    None => net.writers.remove(&sock),
                };
                if let Some(h) = ours {
                    net.graveyard_handles.push(h);
                }
                return Err(e);
            }
            if let Some(old) = displaced {
                net.graveyard_handles.push(old);
            }
            Ok(())
        })?
        .map_err(net::io_error)?;
        self.drain_graveyards(py)?;
        Ok(handle)
    }

    fn remove_writer(&self, py: Python<'_>, fd: u64) -> PyResult<bool> {
        let sock = fd as RawSocket;
        let removed = self
            .with_net(|net, reactor| -> std::io::Result<bool> {
                let removed = net.writers.remove(&sock);
                let had = removed.is_some();
                let r = net.readers.contains_key(&sock);
                if had || r {
                    if let Err(e) = reactor.backend_mut().set_watch(sock, r, false) {
                        // Same restore as remove_reader: see the note there.
                        if let Some(h) = removed {
                            if let Some(newer) = net.writers.insert(sock, h) {
                                net.graveyard_handles.push(newer);
                            }
                        }
                        return Err(e);
                    }
                }
                if let Some(h) = removed {
                    net.graveyard_handles.push(h);
                }
                Ok(had)
            })?
            .map_err(net::io_error)?;
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
    ///
    /// Pending handles' references are reported too, as the loop's own:
    /// handles are not GC-tracked (handles.rs module docs), so a loop that
    /// is dropped without `close()` while a timer's callback references the
    /// loop -- `self.loop = loop; self.h = loop.call_later(...)` -- forms a
    /// cycle the collector could otherwise never see. Only while the state
    /// is quiescent (`peek_for_gc`); `__clear__` then breaks the cycle.
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        for slot in [&self.owner, &self.error_hook, &self.net_error_hook, &self.slow_callback_hook] {
            if let Some(obj) = slot.get() {
                visit.call(obj)?;
            }
        }
        // The cached snapshot's variables may reference the loop (a
        // ContextVar holding it, say): report it, so that cycle is visible.
        if let Some(snap) = self.snaps.peek() {
            visit.call(snap)?;
        }
        if let Some(st) = self.state.peek_for_gc() {
            for h in st.reactor.pending_iter() {
                h.get().visit_refs(&visit)?;
            }
            for h in st.net.readers.values().chain(st.net.writers.values()) {
                h.get().visit_refs(&visit)?;
            }
        }
        Ok(())
    }

    /// The cycle collector found this loop garbage: give up every pending
    /// handle's references, which is what breaks a cycle running through
    /// them (see `__traverse__`).
    fn __clear__(&self, py: Python<'_>) {
        if self.state.is_claimed() {
            return;
        }
        self.snaps.clear(py);
        let Ok(handles) = self.state.with(|st| {
            let mut out: Vec<HandleRef> = st.reactor.clear_pending();
            out.extend(st.net.readers.drain().map(|(_, h)| HandleRef(h)));
            out.extend(st.net.writers.drain().map(|(_, h)| HandleRef(h)));
            out
        }) else {
            return;
        };
        for h in &handles {
            h.get().release(py);
        }
        drop(handles);
    }

    // ---- hooks / config ----------------------------------------------------

    fn set_error_hook(&self, hook: Bound<'_, PyAny>) {
        let _ = self.error_hook.set(hook.unbind());
    }

    fn set_net_error_hook(&self, hook: Bound<'_, PyAny>) {
        let _ = self.net_error_hook.set(hook.unbind());
    }

    /// Seconds, matching asyncio's `loop.slow_callback_duration`.
    fn set_slow_callback_duration(&self, seconds: f64) -> PyResult<()> {
        if !seconds.is_finite() || seconds <= 0.0 {
            return Err(PyValueError::new_err("slow_callback_duration must be a positive number"));
        }
        self.slow_callback_ns.store((seconds * 1e9) as u64, Ordering::Relaxed);
        Ok(())
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
        let (stats, ready, timers, netstats, ops, diag, live_name, foreign) = self.state.with(|st| {
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
                    st.net.stats_sends_posted,
                    st.net.accept_ops_outstanding(),
                    st.net.buffers.stale_rejections(),
                    st.net.reap_guards.len(),
                ),
                net::op_breakdown(&st.net),
                st.reactor.backend_mut().diag(),
                // Live, not the construction-time cache: RIO downgrades its
                // name to "rio-polling" if RIONotify starts failing mid-run.
                st.reactor.backend_name(),
                st.reactor.backend_mut().foreign_completions(),
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
        d.set_item("polls_elided", stats.polls_elided)?;
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
        d.set_item("sends_posted", netstats.8)?;
        d.set_item("accept_ops", netstats.9)?;
        // Buffer ids refused because they were stale (R-073). Non-zero
        // means something held a slot past its release; the pool refused,
        // so no memory was corrupted, but the ownership bug is real.
        d.set_item("stale_buffer_ids", netstats.10)?;
        // Buffers a cancelled op may still own (R-073), awaiting its
        // completion. Non-zero after close() means the kernel had not
        // released them inside the close reap budget, so they are held
        // until the state drops -- correct, but worth seeing.
        d.set_item("unreaped_ops", netstats.11)?;
        // What the live kernel ops are, by target. A stuck loop looks
        // healthy on every other counter; this is the one that says where
        // -- `connect` non-zero with nothing connecting, or `recv` with no
        // live transport, each point at a different failure.
        let by_target = PyDict::new(py);
        by_target.set_item("recv", ops.recv)?;
        by_target.set_item("send", ops.send)?;
        by_target.set_item("accept", ops.accept)?;
        by_target.set_item("connect", ops.connect)?;
        by_target.set_item("dgram", ops.dgram)?;
        by_target.set_item("pipe", ops.pipe)?;
        d.set_item("ops_by_target", by_target)?;
        // Completions that no live operation could own, dropped rather than
        // dereferenced or misattributed: another process's OVERLAPPED on a
        // shared handle (ADR-25), or a second packet for an operation the
        // backend already completed inline. Zero in a correct run.
        d.set_item("foreign_completions", foreign)?;
        if let Some((notifies, reaps)) = diag {
            d.set_item("rio_notifies", notifies)?;
            d.set_item("rio_watchdog_reaps", reaps)?;
        }
        Ok(d)
    }
}

impl CoreLoop {
    /// `deadline_ns` drives the heap; `when` is what `TimerHandle.when()`
    /// reports -- the caller's own float, as asyncio returns it, rather
    /// than the deadline rounded through integer nanoseconds and back.
    fn schedule_timer(
        &self,
        py: Python<'_>,
        deadline_ns: Ticks,
        when: f64,
        base: Handle,
    ) -> PyResult<Py<TimerHandle>> {
        let timer = TimerHandle { when, cancel_counter: Arc::as_ptr(&self.timer_cancels) };
        let handle = Py::new(py, PyClassInitializer::from(base).add_subclass(timer))?;
        // SAFETY: TimerHandle extends Handle (its layout starts with it).
        let payload = HandleRef(
            unsafe { Bound::from_owned_ptr(py, handle.clone_ref(py).into_ptr()).cast_into_unchecked() }
                .unbind(),
        );
        payload.get().mark_scheduled();
        if let Err(f) = self.state.try_with(move |st| st.reactor.schedule_timer(deadline_ns, payload)) {
            // Another thread, while the loop is parked in its poll (the
            // only time the state is out of reach -- gil_boundary): hand
            // the timer over and wake the loop, which may need to park
            // for less time now. asyncio accepts call_later from other
            // threads outside debug mode; the timer just waits for the
            // loop's next iteration there.
            drop(f);
            // SAFETY: as above.
            let payload = HandleRef(
                unsafe { Bound::from_owned_ptr(py, handle.clone_ref(py).into_ptr()).cast_into_unchecked() }
                    .unbind(),
            );
            if self.xqueue.push_timer(deadline_ns, payload) {
                self.waker.wake();
            }
        }
        Ok(handle)
    }

    /// `(deadline_ns, when_secs)` for `call_later(delay)`.
    #[inline]
    fn later_deadline(&self, delay: f64) -> (Ticks, f64) {
        // asyncio: call_at(self.time() + delay), on a FRESH clock -- the
        // tick's cached time went stale behind every callback that ran
        // (or blocked) before this one. The deadline comes from `when`
        // itself, exactly as call_at computes it, so the heap orders
        // timers by the `when()` they report -- past-due ones from a
        // negative delay included, which a clamp to `now` had collapsed
        // into FIFO order.
        let now = self.fresh_ns();
        let when = ticks_to_secs_f64(now) + delay;
        let deadline = if delay.is_nan() { now } else { secs_f64_to_ticks(when) };
        (deadline, when)
    }

    /// Push a freshly built handle on the ready queue; returns it.
    ///
    /// Serves `call_soon` and `call_soon_threadsafe` alike. Whenever the
    /// state is reachable -- the loop thread itself, a loop that is not
    /// running, or another thread while the loop thread is outside its
    /// poll -- the callback goes straight onto the ready queue, behind
    /// everything scheduled before it. That is asyncio's order: one
    /// `_ready` deque for both calls. A separate queue for the threadsafe
    /// call, drained at the next tick, ran `call_soon_threadsafe(a);
    /// call_soon(b)` as b, a -- and charged an eventfd write per call even
    /// on the loop's own thread. Only a loop parked in its poll needs the
    /// cross-thread queue and a wakeup (gil_boundary): there, nothing was
    /// scheduled between the park and this call, so FIFO order holds too.
    #[inline]
    fn enqueue_soon(&self, py: Python<'_>, handle: Handle) -> PyResult<Py<Handle>> {
        let handle = Py::new(py, handle)?;
        let token = HandleRef(handle.clone_ref(py));
        if let Err(f) = self.state.try_with(move |st| st.reactor.push_ready(token)) {
            drop(f);
            if self.xqueue.push(HandleRef(handle.clone_ref(py))) {
                self.waker.wake();
            }
        }
        Ok(handle)
    }

    /// `owner`: the caller's socket object when the descriptor is that
    /// object's (`create_server(sock=...)`); it keeps owning it, and on
    /// failure it is handed back open, as asyncio leaves it.
    fn listen_socket(
        &self,
        py: Python<'_>,
        sock: RawSocket,
        kind: net::ListenerKind,
        accept_pool: usize,
        start: bool,
        owner: Option<Py<PyAny>>,
    ) -> PyResult<(u64, Py<PyAny>, u64)> {
        let sockname = netsys::sockname(sock).ok();
        let owned = owner.is_none();
        let created = self.with_net(|net, reactor| -> std::io::Result<u64> {
            if let Err(e) = reactor.backend_mut().register_socket(sock) {
                // Nothing owns this descriptor yet: `listener_create` has
                // not run, so no listener entry will ever close it. For
                // tcp_listen/http_listen that leaves the port we just
                // bound held for the life of the process.
                if owned {
                    netsys::close(sock);
                } else if let Some(owner) = owner {
                    net.graveyard_py.push(owner);
                }
                return Err(e);
            }
            let lid = net::listener_create(net, sock, kind, accept_pool, owner);
            if start {
                net.now_ns = reactor.now_fresh(); // a failed first post backs off from now
                if let Err(e) = net::listener_start(net, reactor.backend_mut(), lid) {
                    // listener_create already inserted it and the socket is
                    // registered, but no lid reaches the caller -- so the
                    // port would stay bound (and a retry hit EADDRINUSE)
                    // until the whole loop closed. Undo both.
                    net::listener_remove(net, reactor.backend_mut(), lid, false);
                    return Err(e);
                }
            }
            Ok(lid)
        })?;
        self.drain_graveyards(py)?;
        let lid = created.map_err(net::io_error)?;
        // Converted only now: an error above leaves the descriptor closed
        // by the branch that hit it, and nothing after the native work
        // can return early while the socket has no owner.
        let name_obj = sockname
            .map(|a| {
                let t = (a.ip().to_string(), a.port());
                t.into_pyobject(py).map(|b| b.into_any().unbind())
            })
            .transpose()?
            .unwrap_or_else(|| py.None());
        Ok((lid, name_obj, sock as u64))
    }
}

/// `(host, port)` or `(host, port, flowinfo, scope_id)` -> `SocketAddr`.
fn parse_addr_tuple(addr: Bound<'_, PyAny>) -> PyResult<std::net::SocketAddr> {
    let n = addr.len().map_err(|_| {
        PyTypeError::new_err("address must be a (host, port) or (host, port, flowinfo, scope_id) tuple")
    })?;
    if n != 2 && n != 4 {
        return Err(PyValueError::new_err(format!(
            "address must have 2 elements (IPv4) or 4 (IPv6), got {n}"
        )));
    }
    let host: String = addr.get_item(0)?.extract()?;
    let port: u16 = addr.get_item(1)?.extract()?;
    let ip: std::net::IpAddr =
        host.parse().map_err(|_| PyValueError::new_err(format!("invalid IP address: {host:?}")))?;
    if n == 2 {
        return Ok(std::net::SocketAddr::new(ip, port));
    }
    let flowinfo: u32 = addr.get_item(2)?.extract()?;
    let scope_id: u32 = addr.get_item(3)?.extract()?;
    match ip {
        std::net::IpAddr::V6(v6) => {
            Ok(std::net::SocketAddr::V6(std::net::SocketAddrV6::new(v6, port, flowinfo, scope_id)))
        }
        std::net::IpAddr::V4(_) => {
            Err(PyValueError::new_err("a 4-element address is IPv6-only; got an IPv4 host"))
        }
    }
}

/// Everything `bind_listen_socket` needs beyond the address itself.
///
/// A struct rather than more positional parameters: three adjacent bools
/// (`reuse_addr`, `reuse_port`, `tfo`) at a call site are a transposition
/// waiting to happen, and the compiler cannot tell them apart.
struct ListenSpec {
    backlog: i32,
    reuse_addr: bool,
    reuse_port: bool,
    tfo: bool,
    /// IPv6 sockaddr fields; both zero for IPv4 and unscoped IPv6.
    flowinfo: u32,
    scope_id: u32,
}

/// How `bind_listen_socket` failed. Kept apart because asyncio words and
/// handles each differently: create_server skips an address family whose
/// socket cannot be made at all, and names the address in a bind error.
enum ListenError {
    /// Not an IP literal.
    Invalid(PyErr),
    /// socket() itself failed -- typically EAFNOSUPPORT for IPv6 on a host
    /// without it.
    Socket(std::io::Error),
    Bind(std::io::Error),
    /// A socket option or listen().
    Setup(std::io::Error),
}

impl ListenError {
    /// The error asyncio raises for this failure. A bind error names the
    /// address as create_server does --
    /// `error while attempting to bind on address ('::1', 80, 0, 0): ...`
    /// -- since "address already in use" alone does not say which of the
    /// resolved addresses was taken.
    fn into_pyerr(self, py: Python<'_>, ip: &str, port: u16, spec: &ListenSpec) -> PyErr {
        match self {
            ListenError::Invalid(e) => e,
            ListenError::Socket(e) | ListenError::Setup(e) => net::io_error(e),
            ListenError::Bind(e) => {
                let Some(code) = e.raw_os_error() else { return e.into() };
                let sa = if ip.contains(':') {
                    (ip, port, spec.flowinfo, spec.scope_id).into_pyobject(py).map(|t| t.into_any())
                } else {
                    (ip, port).into_pyobject(py).map(|t| t.into_any())
                };
                let repr = match sa.and_then(|t| t.repr()) {
                    Ok(r) => r.to_string(),
                    Err(err) => return err,
                };
                let strerror = net::os_strerror(code).to_lowercase();
                net::os_error_msg(
                    code,
                    format!("error while attempting to bind on address {repr}: {strerror}"),
                )
            }
        }
    }
}

/// Create, bind, and listen a TCP socket (shared by tcp_listen /
/// http_listen).
fn bind_listen_socket(ip: &str, port: u16, spec: &ListenSpec) -> Result<RawSocket, ListenError> {
    let ListenSpec { backlog, reuse_addr, reuse_port, tfo, flowinfo, scope_id } = *spec;
    let addr: std::net::IpAddr = ip
        .parse()
        .map_err(|_| ListenError::Invalid(PyValueError::new_err(format!("invalid IP address: {ip:?}"))))?;
    // A scoped IPv6 bind address (fe80::1%eth0) resolves to a sockaddr
    // whose scope_id names the interface. Dropping it bound with scope
    // zero, so a server aimed at a link-local interface bound the wrong
    // thing or failed outright, despite resolving cleanly.
    let sockaddr = match addr {
        std::net::IpAddr::V6(v6) => {
            std::net::SocketAddr::V6(std::net::SocketAddrV6::new(v6, port, flowinfo, scope_id))
        }
        std::net::IpAddr::V4(_) => std::net::SocketAddr::new(addr, port),
    };
    let family = if sockaddr.is_ipv4() { netsys::AF_INET } else { netsys::AF_INET6 };
    let sock = netsys::create_tcp(family).map_err(ListenError::Socket)?;
    let setup = (|| -> Result<(), ListenError> {
        if family == netsys::AF_INET6 {
            // A wildcard IPv6 socket accepts IPv4 too on Linux (v6only
            // defaults off), so `create_server(host=None, port=P)` --
            // which resolves to BOTH 0.0.0.0 and :: and binds a listener
            // per family -- had whichever bound second fail EADDRINUSE,
            // and the Python layer then rolled back an otherwise valid
            // server. Separate per-family listeners require this.
            netsys::set_v6only(sock, true).map_err(ListenError::Setup)?;
        }
        if reuse_addr {
            netsys::set_reuse_addr(sock, true).map_err(ListenError::Setup)?;
        }
        if reuse_port {
            netsys::set_reuse_port(sock, true).map_err(ListenError::Setup)?;
        }
        if tfo {
            // R-038. Not best-effort: the option was asked for
            // explicitly, and silently serving without it is how it came
            // to be a no-op in the first place.
            netsys::set_fastopen(sock, backlog).map_err(ListenError::Setup)?;
        }
        netsys::bind(sock, &netsys::build_sockaddr(sockaddr)).map_err(ListenError::Bind)?;
        netsys::listen(sock, backlog).map_err(ListenError::Setup)
    })();
    if let Err(e) = setup {
        netsys::close(sock);
        return Err(e);
    }
    Ok(sock)
}

// --------------------------------------------------------------------- //
// raw FASTCALL scheduling entry points (see `CoreLoop::fast_methods`)    //
// --------------------------------------------------------------------- //

/// `"context"`, interned once: asyncio's C Task/Future pass it in a static
/// kwnames tuple whose item is the interned string, so the common case is
/// a pointer compare.
static CONTEXT_KW: PyOnceLock<Py<PyString>> = PyOnceLock::new();

/// The single keyword every scheduling call takes: `context=`.
///
/// # Safety
/// `args`/`kwnames` must be a valid vectorcall argument array: `nargs`
/// positionals followed by one value per name in `kwnames`.
#[inline]
unsafe fn sched_context_kw(
    py: Python<'_>,
    args: *const *mut ffi::PyObject,
    nargs: usize,
    kwnames: *mut ffi::PyObject,
    fname: &str,
) -> PyResult<*mut ffi::PyObject> {
    if kwnames.is_null() {
        return Ok(std::ptr::null_mut());
    }
    let ctx_name = CONTEXT_KW.get_or_init(py, || PyString::intern(py, "context").unbind()).as_ptr();
    let mut context = std::ptr::null_mut();
    unsafe {
        let nkw = ffi::PyTuple_GET_SIZE(kwnames);
        for i in 0..nkw {
            let key = ffi::PyTuple_GET_ITEM(kwnames, i);
            let matches = key == ctx_name || ffi::PyUnicode_Compare(key, ctx_name) == 0;
            if !matches {
                let key = Bound::from_borrowed_ptr(py, key);
                return Err(PyTypeError::new_err(format!(
                    "{fname}() got an unexpected keyword argument {}",
                    key.repr()?
                )));
            }
            context = *args.add(nargs + i as usize);
        }
    }
    Ok(context)
}

/// Build a Handle from raw vectorcall arguments `args[0..n]` (callback
/// first). `context` is borrowed and may be null or None (copy current).
///
/// # Safety
/// `args` must hold `n >= 1` valid borrowed pointers; `context` must be
/// null or a valid borrowed pointer.
#[inline]
unsafe fn raw_handle(
    py: Python<'_>,
    core: &CoreLoop,
    args: *const *mut ffi::PyObject,
    n: usize,
    context: *mut ffi::PyObject,
    method: &str,
) -> PyResult<Handle> {
    unsafe {
        let callback = Borrowed::from_ptr(py, *args);
        let rest = CallArgs::from_slice(py, args.add(1), n - 1)?;
        let ctx = if context.is_null() { None } else { Some(Borrowed::from_ptr(py, context).to_owned()) };
        core.make_handle(py, &callback, rest, ctx.as_ref(), method)
    }
}

/// `missing 1 required positional argument` in CPython's wording.
fn missing_arg(fname: &str, what: &str) -> PyErr {
    PyTypeError::new_err(format!("{fname}() missing 1 required positional argument: '{what}'"))
}

/// A float argument (`delay` / `when`) the way `float(x)` reads it.
///
/// # Safety
/// `obj` must be a valid borrowed pointer.
#[inline]
unsafe fn arg_f64(py: Python<'_>, obj: *mut ffi::PyObject) -> PyResult<f64> {
    unsafe {
        if ffi::PyFloat_CheckExact(obj) != 0 {
            return Ok(ffi::PyFloat_AS_DOUBLE(obj));
        }
        let v = ffi::PyFloat_AsDouble(obj);
        if v == -1.0 && !ffi::PyErr_Occurred().is_null() {
            return Err(PyErr::fetch(py));
        }
        Ok(v)
    }
}

unsafe fn fast_call_soon(
    py: Python<'_>,
    slf: *mut ffi::PyObject,
    args: *const *mut ffi::PyObject,
    nargs: ffi::Py_ssize_t,
    kwnames: *mut ffi::PyObject,
) -> PyResult<*mut ffi::PyObject> {
    let nargs = nargs as usize;
    unsafe {
        let core = Borrowed::from_ptr(py, slf).cast_unchecked::<CoreLoop>();
        let this = core.get();
        if nargs == 0 {
            return Err(missing_arg("call_soon", "callback"));
        }
        let context = sched_context_kw(py, args, nargs, kwnames, "call_soon")?;
        this.check_closed()?;
        this.check_thread()?;
        let handle = raw_handle(py, this, args, nargs, context, "call_soon")?;
        Ok(this.enqueue_soon(py, handle)?.into_ptr())
    }
}

unsafe fn fast_call_soon_threadsafe(
    py: Python<'_>,
    slf: *mut ffi::PyObject,
    args: *const *mut ffi::PyObject,
    nargs: ffi::Py_ssize_t,
    kwnames: *mut ffi::PyObject,
) -> PyResult<*mut ffi::PyObject> {
    let nargs = nargs as usize;
    unsafe {
        let core = Borrowed::from_ptr(py, slf).cast_unchecked::<CoreLoop>();
        let this = core.get();
        if nargs == 0 {
            return Err(missing_arg("call_soon_threadsafe", "callback"));
        }
        let context = sched_context_kw(py, args, nargs, kwnames, "call_soon_threadsafe")?;
        this.check_closed()?;
        let handle = raw_handle(py, this, args, nargs, context, "call_soon_threadsafe")?;
        Ok(this.enqueue_soon(py, handle)?.into_ptr())
    }
}

unsafe fn fast_call_later(
    py: Python<'_>,
    slf: *mut ffi::PyObject,
    args: *const *mut ffi::PyObject,
    nargs: ffi::Py_ssize_t,
    kwnames: *mut ffi::PyObject,
) -> PyResult<*mut ffi::PyObject> {
    let nargs = nargs as usize;
    unsafe {
        let core = Borrowed::from_ptr(py, slf).cast_unchecked::<CoreLoop>();
        let this = core.get();
        if nargs == 0 {
            return Err(missing_arg("call_later", "delay"));
        }
        if nargs == 1 {
            return Err(missing_arg("call_later", "callback"));
        }
        let context = sched_context_kw(py, args, nargs, kwnames, "call_later")?;
        let delay = arg_f64(py, *args)?;
        this.check_closed()?;
        this.check_thread()?;
        let (deadline, when) = this.later_deadline(delay);
        // "call_at" in its error messages, as asyncio's (see call_later).
        let base = raw_handle(py, this, args.add(1), nargs - 1, context, "call_at")?;
        Ok(this.schedule_timer(py, deadline, when, base)?.into_ptr())
    }
}

unsafe fn fast_call_at(
    py: Python<'_>,
    slf: *mut ffi::PyObject,
    args: *const *mut ffi::PyObject,
    nargs: ffi::Py_ssize_t,
    kwnames: *mut ffi::PyObject,
) -> PyResult<*mut ffi::PyObject> {
    let nargs = nargs as usize;
    unsafe {
        let core = Borrowed::from_ptr(py, slf).cast_unchecked::<CoreLoop>();
        let this = core.get();
        if nargs == 0 {
            return Err(missing_arg("call_at", "when"));
        }
        if nargs == 1 {
            return Err(missing_arg("call_at", "callback"));
        }
        let context = sched_context_kw(py, args, nargs, kwnames, "call_at")?;
        let when = arg_f64(py, *args)?;
        this.check_closed()?;
        this.check_thread()?;
        let base = raw_handle(py, this, args.add(1), nargs - 1, context, "call_at")?;
        Ok(this.schedule_timer(py, secs_f64_to_ticks(when), when, base)?.into_ptr())
    }
}

/// `PyMethodDef`s for the FASTCALL entry points. In a static: CPython keeps
/// the pointer for the life of every function object made from it.
struct FastDefs([ffi::PyMethodDef; 4]);

// SAFETY: immutable after construction; the raw pointers are to 'static
// C strings and functions.
unsafe impl Sync for FastDefs {}

macro_rules! fast_def {
    ($name:literal, $f:path, $doc:literal) => {
        ffi::PyMethodDef {
            ml_name: $name.as_ptr(),
            ml_meth: ffi::PyMethodDefPointer {
                // PyO3's own FASTCALL trampoline: it maintains PyO3's
                // attach count, converts panics, and restores errors --
                // exactly what the #[pymethods] wrappers get, minus the
                // generic argument extraction.
                PyCFunctionFastWithKeywords: pyo3::impl_::trampoline::get_trampoline_function!(
                    fastcall_cfunction_with_keywords,
                    $f
                ),
            },
            ml_flags: ffi::METH_FASTCALL | ffi::METH_KEYWORDS,
            ml_doc: $doc.as_ptr(),
        }
    };
}

static FAST_DEFS: FastDefs = FastDefs([
    fast_def!(c"call_soon", fast_call_soon, c"call_soon(callback, *args, context=None)"),
    fast_def!(
        c"call_soon_threadsafe",
        fast_call_soon_threadsafe,
        c"call_soon_threadsafe(callback, *args, context=None)"
    ),
    fast_def!(c"call_later", fast_call_later, c"call_later(delay, callback, *args, context=None)"),
    fast_def!(c"call_at", fast_call_at, c"call_at(when, callback, *args, context=None)"),
]);
