//! L1 reactor (R-021, R-030 timeout policy, R-054 batching, R-060
//! spin-then-park, R-061 cached clock).
//!
//! The reactor is generic over an opaque callback token `T` (the pyshim
//! layer uses `Py<Handle>`). It owns the clock, timer heap, ready queue,
//! cross-thread queue, and the L0 backend. It does NOT dispatch callbacks —
//! the binding layer pops tokens (GIL held) and invokes them, so this crate
//! stays Python-free.
//!
//! Tick shape (driven by the binding layer):
//!
//! ```text
//! loop {
//!     reactor.prepare_tick()            // GIL held: clock, xthread drain, timers
//!     let t = reactor.poll_timeout();
//!     reactor.poll(t)                   // GIL RELEASED (R-021)
//!     reactor.finish_poll()             // GIL held: xthread drain, timers
//!     while let Some(tok) = reactor.pop_ready_batched() {
//!         dispatch(tok)                 // GIL held, one re-acquire per batch
//!     }
//! }
//! ```

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backend::{self, BackendKind, Completion, IoBackend, Wakeup};
use crate::ready::{CrossThreadQueue, ReadyQueue, DISPATCH_BATCH};
use crate::time::{Clock, Ticks};
use crate::timer::{TimerHeap, TimerToken};

/// R-030: poll timeout clamp.
const MAX_PARK: Duration = Duration::from_millis(100);

#[derive(Debug, Clone)]
pub struct ReactorConfig {
    pub backend: BackendKind,
    /// R-060: busy-poll window before parking, microseconds. 0 disables.
    pub spin_us: u64,
    /// R-030: completion batch size per GetQueuedCompletionStatusEx call.
    pub poll_batch: usize,
}

impl Default for ReactorConfig {
    fn default() -> Self {
        ReactorConfig { backend: BackendKind::Auto, spin_us: 20, poll_batch: 256 }
    }
}

/// Counters surfaced through `loop.stats()` (R-103).
#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub ticks: u64,
    pub completions: u64,
    pub callbacks_dispatched: u64,
    pub timers_fired: u64,
    pub xthread_items: u64,
    pub spin_hits: u64,
    pub polls: u64,
}

pub struct Reactor<T> {
    clock: Clock,
    timers: TimerHeap<T>,
    ready: ReadyQueue<T>,
    xthread: Arc<CrossThreadQueue<T>>,
    backend: Box<dyn IoBackend + Send>,
    wakeup: Arc<dyn Wakeup>,
    /// Timer cancellations observed since last tick (see
    /// `TimerHandleShared`): drained into the heap's tombstone accounting.
    cancelled_timers: Arc<AtomicUsize>,
    completions: Vec<Completion>,
    /// Tokens delivered by I/O completions, mapped op->token by the
    /// transport layer (M1); unused pre-M1 but part of the tick contract.
    pub stats: Stats,
    cfg: ReactorConfig,
    /// Remaining items allowed in the current dispatch batch (R-054).
    batch_left: usize,
    /// Timestamp of the last REAL backend poll (see `poll`'s skip window).
    last_poll_ns: Ticks,
    /// Ready-queue length snapshot at tick start: callbacks scheduled during
    /// dispatch never run in the same tick (asyncio `_run_once` semantics).
    ready_snapshot: usize,
}

impl<T> Reactor<T> {
    pub fn new(cfg: ReactorConfig) -> io::Result<Self> {
        let backend = backend::create(cfg.backend)?;
        let wakeup = backend.wakeup_handle();
        Ok(Reactor {
            clock: Clock::new(),
            timers: TimerHeap::new(),
            ready: ReadyQueue::new(),
            xthread: Arc::new(CrossThreadQueue::new()),
            backend,
            wakeup,
            cancelled_timers: Arc::new(AtomicUsize::new(0)),
            completions: Vec::with_capacity(cfg.poll_batch),
            stats: Stats::default(),
            cfg,
            batch_left: 0,
            last_poll_ns: 0,
            ready_snapshot: 0,
        })
    }

    pub fn backend_name(&self) -> &'static str {
        self.backend.name()
    }

    /// Direct backend access for the transport layer (post ops, watches,
    /// accept-socket retrieval). Loop-thread only, like all `&mut self`.
    pub fn backend_mut(&mut self) -> &mut (dyn IoBackend + Send) {
        &mut *self.backend
    }

    // ---- time -----------------------------------------------------------

    /// Cached tick timestamp (R-061). Refreshed by `prepare_tick`; callers
    /// outside a running tick should use `now_fresh`.
    #[inline]
    pub fn time_cached(&self) -> Ticks {
        self.clock.cached()
    }

    #[inline]
    pub fn now_fresh(&mut self) -> Ticks {
        self.clock.refresh()
    }

    // ---- scheduling (loop thread) ----------------------------------------

    pub fn push_ready(&mut self, token: T) {
        self.ready.push(token);
    }

    pub fn schedule_timer(&mut self, deadline: Ticks, token: T) -> Arc<TimerToken> {
        self.timers.schedule(deadline, token)
    }

    /// Schedule with a caller-provided cancellation token (the binding
    /// layer constructs its `TimerHandle` object — which owns the token —
    /// before entering the state critical section).
    pub fn schedule_timer_with_token(&mut self, deadline: Ticks, token: T, cancel: Arc<TimerToken>) {
        self.timers.schedule_with_token(deadline, token, cancel);
    }

    /// Tokens discarded by tombstone removal/compaction since the last
    /// call. The caller MUST drain and drop these OUTSIDE its state
    /// critical section (dropping a Python ref can run `__del__`/GC).
    pub fn take_graveyard(&mut self) -> Vec<T> {
        self.timers.take_graveyard()
    }

    /// Shared counter bumped by `TimerHandle.cancel()`; consumed each tick
    /// to drive tombstone-ratio compaction (R-053) without needing `&mut`
    /// access to the heap from handle objects.
    pub fn timer_cancel_counter(&self) -> Arc<AtomicUsize> {
        self.cancelled_timers.clone()
    }

    // ---- scheduling (any thread) ------------------------------------------

    /// Handles for `call_soon_threadsafe`: (queue, wakeup). Producers push,
    /// then wake iff push returned true (R-022).
    pub fn cross_thread_handles(&self) -> (Arc<CrossThreadQueue<T>>, Arc<dyn Wakeup>) {
        (self.xthread.clone(), self.wakeup.clone())
    }

    // ---- tick ------------------------------------------------------------

    /// Start-of-tick bookkeeping (GIL held): refresh clock, absorb timer
    /// cancellations, drain the cross-thread queue, fire expired timers.
    pub fn prepare_tick(&mut self) {
        self.stats.ticks += 1;
        self.clock.refresh();
        self.absorb_cancellations();
        self.drain_xthread();
        self.fire_expired_timers();
        self.ready_snapshot = self.ready.len();
        self.batch_left = DISPATCH_BATCH.min(self.ready_snapshot);
    }

    fn absorb_cancellations(&mut self) {
        let n = self.cancelled_timers.swap(0, Ordering::AcqRel);
        for _ in 0..n {
            self.timers.note_cancelled();
        }
    }

    fn drain_xthread(&mut self) {
        self.stats.xthread_items += self.xthread.drain_into(&mut self.ready) as u64;
    }

    fn fire_expired_timers(&mut self) {
        let now = self.clock.cached();
        while let Some(token) = self.timers.pop_expired(now) {
            self.ready.push(token);
            self.stats.timers_fired += 1;
        }
    }

    /// R-030: 0 when ready callbacks are pending; else time to the next
    /// timer deadline; clamped to [0, 100ms].
    pub fn poll_timeout(&mut self) -> Duration {
        if !self.ready.is_empty() {
            return Duration::ZERO;
        }
        let now = self.clock.cached();
        match self.timers.next_deadline() {
            Some(d) if d <= now => Duration::ZERO,
            Some(d) => Duration::from_nanos(d - now).min(MAX_PARK),
            None => MAX_PARK,
        }
    }

    /// Poll the backend. MUST be called with the GIL released (R-021).
    /// Spin-then-park (R-060): busy-poll with zero timeout for `spin_us`
    /// before parking, unless the timeout is already zero.
    pub fn poll(&mut self, timeout: Duration) -> io::Result<()> {
        self.completions.clear();
        if timeout.is_zero() {
            // Poll-skip window (adopted from rloop): with ready callbacks
            // pending, an actual kernel poll is only taken every 250us —
            // bounded I/O-discovery staleness under CPU saturation in
            // exchange for skipping the epoll_wait/GQCSEx syscall on the
            // vast majority of busy ticks. Parked polls (timeout > 0) are
            // never skipped, so idle latency is unaffected.
            let now = self.clock.cached();
            if now.saturating_sub(self.last_poll_ns) < 250_000 {
                return Ok(());
            }
            self.stats.polls += 1;
            self.backend.try_poll(&mut self.completions)?;
            self.last_poll_ns = now;
            return Ok(());
        }
        self.stats.polls += 1;
        if self.cfg.spin_us > 0 {
            let spin_budget = Duration::from_micros(self.cfg.spin_us).min(timeout);
            let spin_start = std::time::Instant::now();
            while spin_start.elapsed() < spin_budget {
                if self.backend.try_poll(&mut self.completions)? > 0 || !self.xthread.is_empty() {
                    self.stats.spin_hits += 1;
                    return Ok(());
                }
                std::hint::spin_loop();
            }
        }
        self.backend.poll(&mut self.completions, Some(timeout))?;
        self.last_poll_ns = self.clock.cached();
        Ok(())
    }

    /// End-of-poll bookkeeping (GIL held): count completions, drain the
    /// cross-thread queue (a Wakeup completion means producers posted),
    /// fire timers that expired while parked, and snapshot the dispatch
    /// batch.
    ///
    /// `parked` = the poll may have blocked: refresh the tick clock so
    /// timers fired below use post-park time. A zero-timeout poll keeps
    /// `prepare_tick`'s timestamp — R-061 requires (at least) one refresh
    /// per tick, and skipping the redundant read saves a clock syscall on
    /// the hot path.
    pub fn finish_poll_after(&mut self, parked: bool) -> &[Completion] {
        self.stats.completions += self.completions.len() as u64;
        if parked {
            self.clock.refresh();
        }
        self.absorb_cancellations();
        self.drain_xthread();
        self.fire_expired_timers();
        let batch = DISPATCH_BATCH.min(self.ready.len());
        self.ready_snapshot = self.ready.len();
        self.batch_left = batch;
        &self.completions
    }

    pub fn finish_poll(&mut self) -> &[Completion] {
        self.finish_poll_after(true)
    }

    /// Copy this tick's completions out (transport translation happens in
    /// the binding layer, which also owns the op->target mapping).
    pub fn drain_completions(&mut self, out: &mut Vec<Completion>) {
        out.extend_from_slice(&self.completions);
        self.completions.clear();
    }

    /// Pop the next token of the current dispatch batch (R-054: max 128 per
    /// drain; never more than were queued when the batch started, matching
    /// asyncio's `_run_once` snapshot semantics).
    pub fn pop_ready_batched(&mut self) -> Option<T> {
        if self.batch_left == 0 {
            return None;
        }
        self.batch_left -= 1;
        let token = self.ready.pop();
        if token.is_some() {
            self.stats.callbacks_dispatched += 1;
        }
        token
    }

    pub fn ready_len(&self) -> usize {
        self.ready.len()
    }

    pub fn timers_len(&self) -> usize {
        self.timers.len()
    }

    /// `loop.close()` teardown: extract all queued work and return it so
    /// the caller can drop it at a safe point (see `take_graveyard`).
    /// In-flight kernel-op cancellation/drain (R-122 "close with pending
    /// ops") is layered in the transport integration (M1); the portable
    /// backend has no kernel ops.
    #[must_use = "returned tokens must be dropped outside any state critical section"]
    pub fn clear_pending(&mut self) -> Vec<T> {
        let mut out = Vec::new();
        while let Some(t) = self.ready.pop() {
            out.push(t);
        }
        let mut scratch = ReadyQueue::new();
        self.xthread.drain_into(&mut scratch);
        while let Some(t) = scratch.pop() {
            out.push(t);
        }
        while let Some(t) = self.timers.pop_expired(u64::MAX) {
            out.push(t);
        }
        out.extend(self.timers.take_graveyard());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reactor() -> Reactor<u32> {
        Reactor::new(ReactorConfig { spin_us: 0, ..Default::default() }).unwrap()
    }

    fn run_tick(r: &mut Reactor<u32>) -> Vec<u32> {
        r.prepare_tick();
        let t = r.poll_timeout();
        r.poll(t).unwrap();
        r.finish_poll();
        let mut out = Vec::new();
        while let Some(tok) = r.pop_ready_batched() {
            out.push(tok);
        }
        out
    }

    #[test]
    fn call_soon_order_preserved() {
        let mut r = reactor();
        for i in 0..5 {
            r.push_ready(i);
        }
        assert_eq!(run_tick(&mut r), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn timers_fire_in_order_after_deadline() {
        let mut r = reactor();
        let now = r.now_fresh();
        r.schedule_timer(now + 5_000_000, 2); // +5ms
        r.schedule_timer(now + 1_000_000, 1); // +1ms
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(run_tick(&mut r), vec![1, 2]);
    }

    #[test]
    fn cancelled_timer_never_fires() {
        let mut r = reactor();
        let now = r.now_fresh();
        let tok = r.schedule_timer(now + 1_000_000, 1);
        r.schedule_timer(now + 2_000_000, 2);
        tok.cancel();
        r.timer_cancel_counter().fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(run_tick(&mut r), vec![2]);
    }

    #[test]
    fn poll_timeout_tracks_next_timer() {
        let mut r = reactor();
        r.prepare_tick();
        assert_eq!(r.poll_timeout(), MAX_PARK, "idle loop parks at clamp");
        let now = r.time_cached();
        r.schedule_timer(now + 3_000_000, 1);
        let t = r.poll_timeout();
        assert!(t <= Duration::from_millis(3));
        assert!(t > Duration::ZERO);
        r.push_ready(9);
        assert_eq!(r.poll_timeout(), Duration::ZERO, "ready work forces zero timeout");
    }

    #[test]
    fn threadsafe_wakeup_delivers_token() {
        let mut r = reactor();
        let (q, wake) = r.cross_thread_handles();
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            if q.push(42) {
                wake.wake();
            }
        });
        r.prepare_tick();
        let timeout = r.poll_timeout();
        let start = std::time::Instant::now();
        r.poll(timeout).unwrap();
        r.finish_poll();
        let mut out = Vec::new();
        while let Some(tok) = r.pop_ready_batched() {
            out.push(tok);
        }
        // One 100ms park may elapse before the wakeup posts; run a second
        // tick if needed (the wakeup is never lost).
        if out.is_empty() {
            out = run_tick(&mut r);
        }
        assert_eq!(out, vec![42]);
        assert!(start.elapsed() < Duration::from_secs(2));
        t.join().unwrap();
    }

    #[test]
    fn batch_snapshot_defers_reentrant_pushes() {
        let mut r = reactor();
        for i in 0..3 {
            r.push_ready(i);
        }
        r.prepare_tick();
        r.poll(Duration::ZERO).unwrap();
        r.finish_poll();
        let first = r.pop_ready_batched().unwrap();
        assert_eq!(first, 0);
        // Simulate a callback scheduling more work mid-dispatch.
        r.push_ready(99);
        let mut rest = Vec::new();
        while let Some(tok) = r.pop_ready_batched() {
            rest.push(tok);
        }
        assert_eq!(rest, vec![1, 2], "token 99 must wait for the next tick");
        assert_eq!(run_tick(&mut r), vec![99]);
    }

    #[test]
    fn dispatch_batch_caps_at_128() {
        let mut r = reactor();
        for i in 0..300 {
            r.push_ready(i);
        }
        let first = run_tick(&mut r);
        assert_eq!(first.len(), DISPATCH_BATCH);
        assert_eq!(run_tick(&mut r).len(), DISPATCH_BATCH);
        assert_eq!(run_tick(&mut r).len(), 300 - 2 * DISPATCH_BATCH);
    }

    #[test]
    fn clear_pending_empties_everything() {
        let mut r = reactor();
        r.push_ready(1);
        let now = r.now_fresh();
        r.schedule_timer(now + 1_000_000_000, 2);
        let (q, _w) = r.cross_thread_handles();
        let _ = q.push(3);
        let mut drained = r.clear_pending();
        drained.sort();
        assert_eq!(drained, vec![1, 2, 3]);
        assert_eq!(r.ready_len(), 0);
        assert_eq!(r.timers_len(), 0);
        assert!(run_tick(&mut r).is_empty());
    }
}
