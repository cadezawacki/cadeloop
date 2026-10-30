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
//!     reactor.take_batch(&mut batch);   // the ready snapshot, I/O watches included
//!     let timers = reactor.take_expired(&mut batch);
//!     dispatch(batch)                   // GIL held: the snapshot, then the
//!                                       // tick's transport events, then the
//!                                       // `timers` expired timers at its tail
//! }
//! ```

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backend::{self, BackendKind, Completion, IoBackend, Wakeup};
use crate::ready::{CrossThreadQueue, ReadyQueue, INITIAL_BATCH_CAPACITY};
use crate::spin_gate::SpinGate;
use crate::time::{Clock, Ticks};
use crate::timer::{TimerHeap, TimerPayload};

/// R-030: poll timeout clamp.
const MAX_PARK: Duration = Duration::from_millis(100);

/// Poll-skip window (adopted from rloop): with ready callbacks pending, a
/// real kernel poll is only taken this often.
const POLL_SKIP_NS: Ticks = 250_000;

#[derive(Debug, Clone)]
pub struct ReactorConfig {
    pub backend: BackendKind,
    /// R-060: busy-poll window before parking, microseconds. 0 disables.
    pub spin_us: u64,
    /// R-060: spin only while spins are finding work (`spin_gate`), rather
    /// than before every park.
    pub adaptive_spin: bool,
    /// R-030: completion batch size per GetQueuedCompletionStatusEx call.
    pub poll_batch: usize,
    /// R-041/R-042: RIO completion/request queue sizing.
    pub backend_opts: backend::BackendOptions,
    /// Added to the platform clock so ticks line up with Python's
    /// `time.monotonic()` (0 where they already do; see `time`).
    pub clock_offset_ns: i64,
}

impl Default for ReactorConfig {
    fn default() -> Self {
        ReactorConfig {
            backend: BackendKind::Auto,
            spin_us: 20,
            adaptive_spin: false,
            poll_batch: 256,
            backend_opts: backend::BackendOptions::default(),
            clock_offset_ns: 0,
        }
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
    /// Parks preceded by a spin (each one of `polls`).
    pub spins: u64,
    /// Spins that work arrived during: the parks spinning saved. Work
    /// already waiting when a spin began is not counted -- the park would
    /// have returned as fast.
    pub spin_hits: u64,
    /// Zero-timeout polls skipped outright because the backend had no
    /// possible completion source (has_io_interest() == false).
    pub polls_elided: u64,
    pub polls: u64,
    /// Parks bounded by a timer's deadline that used the backend's precise
    /// wait (IOCP: the high-resolution park timer).
    pub precise_parks: u64,
}

pub struct Reactor<T> {
    clock: Clock,
    timers: TimerHeap<T>,
    ready: ReadyQueue<T>,
    xthread: Arc<CrossThreadQueue<T>>,
    backend: Box<dyn IoBackend + Send>,
    wakeup: Arc<dyn Wakeup>,
    /// Cancellations of still-scheduled timers reported since the last
    /// tick (the binding layer's `TimerHandle.cancel()` bumps it): drained
    /// into the heap's tombstone accounting.
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
    /// The dispatch buffer `take_batch` trades with the ready queue, lent
    /// to the binding layer for the length of a run (`take_spare`).
    spare: VecDeque<T>,
    /// Timers that expired this tick, in deadline order, until the binding
    /// layer takes them behind the ready batch (`take_expired`).
    expired: VecDeque<T>,
    /// The last `poll_timeout` ends at a timer's deadline (not the idle
    /// clamp): the park should end on time (`IoBackend::poll_precise`).
    deadline_bound: bool,
    /// R-060 adaptive spinning (`ReactorConfig::adaptive_spin`).
    spin_gate: SpinGate,
}

impl<T: TimerPayload> Reactor<T> {
    pub fn new(cfg: ReactorConfig) -> io::Result<Self> {
        let backend = backend::create(cfg.backend, &cfg.backend_opts)?;
        let wakeup = backend.wakeup_handle();
        Ok(Reactor {
            clock: Clock::with_offset(cfg.clock_offset_ns),
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
            spare: VecDeque::with_capacity(INITIAL_BATCH_CAPACITY),
            expired: VecDeque::new(),
            deadline_bound: false,
            spin_gate: SpinGate::new(),
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

    /// The tick's timestamp (R-061): read on first use after
    /// `prepare_tick` (see `time`); callers outside a running tick should
    /// use `now_fresh`.
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

    /// Undo a `pop_ready_batched`: return tokens to the queue FRONT so
    /// FIFO order survives an exceptional unwind (e.g. KeyboardInterrupt
    /// between batch take and dispatch). Feed tokens in REVERSE pop order.
    pub fn unpop_ready(&mut self, token: T) {
        self.ready.push_front(token);
        self.batch_left += 1;
        self.stats.callbacks_dispatched = self.stats.callbacks_dispatched.saturating_sub(1);
    }

    /// Undo a `take_expired` for a timer an unwind left undispatched: back
    /// to the FRONT of the expired timers, ahead of any that expire later.
    /// Feed tokens in REVERSE order.
    pub fn unpop_expired(&mut self, token: T) {
        self.expired.push_front(token);
    }

    /// Schedule `token` to become ready at `deadline`. Cancellation is read
    /// off the payload itself ([`TimerPayload::is_cancelled`]).
    #[inline]
    pub fn schedule_timer(&mut self, deadline: Ticks, token: T) {
        self.timers.schedule(deadline, token);
    }

    /// Tokens discarded by tombstone removal/compaction since the last
    /// call. The caller MUST drain and drop these OUTSIDE its state
    /// critical section (dropping a Python ref can run `__del__`/GC).
    pub fn take_graveyard(&mut self) -> Vec<T> {
        self.timers.take_graveyard()
    }

    /// Cheap test for `take_graveyard` having anything to hand back.
    #[inline]
    pub fn has_graveyard(&self) -> bool {
        self.timers.has_graveyard()
    }

    /// Shared counter bumped by `TimerHandle.cancel()` (for still-scheduled
    /// timers only); consumed each tick to drive tombstone-ratio compaction
    /// (R-053) without needing `&mut` access to the heap from handle
    /// objects.
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

    /// R-052: mark/clear the signal-wakeup fd on the backend so poll
    /// elision does not count it as I/O interest.
    pub fn set_wake_only(&mut self, fd: Option<crate::backend::RawSocket>) {
        self.backend.set_wake_only(fd);
    }

    /// Start-of-tick bookkeeping (GIL held): refresh clock, absorb timer
    /// cancellations, drain the cross-thread queue, collect expired timers.
    pub fn prepare_tick(&mut self) {
        self.stats.ticks += 1;
        self.clock.invalidate();
        self.absorb_cancellations();
        self.drain_xthread();
        self.fire_expired_timers();
        self.snapshot_batch();
    }

    /// Make this tick's dispatch batch everything queued right now.
    ///
    /// No cap: asyncio's `_run_once` runs every callback that was ready
    /// when the iteration began, and `stop()` takes effect only once that
    /// iteration is over. The 128-callback cap this replaced made
    /// `run_forever` return after 127 of 300 callbacks queued behind a
    /// `call_soon(loop.stop)`, where asyncio and uvloop run all 300.
    #[inline]
    fn snapshot_batch(&mut self) {
        self.ready_snapshot = self.ready.len();
        self.batch_left = self.ready_snapshot;
    }

    /// Fold callbacks queued since the snapshot -- the readiness watches
    /// this tick's poll just reported -- into the current batch, BEFORE any
    /// of it is dispatched.
    ///
    /// asyncio appends an iteration's I/O callbacks to the ready queue and
    /// runs them in that same iteration. Left for the next tick instead,
    /// they ran one tick late, and that tick's poll found the fd still
    /// readable (nothing had read it yet) and queued the watch a second
    /// time: one readiness event, two calls -- a reader that drained its
    /// socket on the first call hit `BlockingIOError` on the second.
    pub fn absorb_into_batch(&mut self) {
        debug_assert_eq!(self.batch_left, self.ready_snapshot, "absorb after dispatch began");
        self.snapshot_batch();
    }

    #[inline]
    fn absorb_cancellations(&mut self) {
        // Load first: the swap is a locked read-modify-write, and on the
        // overwhelmingly common tick nothing was cancelled at all. Paying
        // it unconditionally (twice a tick) was a measurable share of a
        // pure-scheduling tick.
        if self.cancelled_timers.load(Ordering::Relaxed) == 0 {
            return;
        }
        let n = self.cancelled_timers.swap(0, Ordering::AcqRel);
        if n > 0 {
            self.timers.note_cancelled(n);
        }
    }

    #[inline]
    fn drain_xthread(&mut self) {
        let timers = &mut self.timers;
        let n = self.xthread.drain_into(&mut self.ready, |deadline, token| timers.schedule(deadline, token));
        self.stats.xthread_items += n as u64;
    }

    /// Move the timers due now out of the heap, behind any collected
    /// earlier this tick. They are dispatched after the tick's ready batch
    /// AND its I/O callbacks (`take_expired`).
    #[inline]
    fn fire_expired_timers(&mut self) {
        // Cheap reject before the pop loop: the top entry is a lower bound
        // for every deadline in the heap (cancelled or not). An empty heap
        // needs no clock read at all.
        let Some(top) = self.timers.peek_deadline() else { return };
        let now = self.clock.cached();
        if top > now {
            return;
        }
        while let Some(token) = self.timers.pop_expired(now) {
            self.expired.push_back(token);
            self.stats.timers_fired += 1;
        }
    }

    /// Append this tick's expired timers to `out` -- the dispatch batch,
    /// once `take_batch` has filled it -- and return how many: the binding
    /// layer runs them after the batch's other callbacks and after the
    /// tick's transport events.
    ///
    /// That is asyncio's `_run_once` order: the previous iteration's
    /// callbacks, then the I/O callbacks `_process_events` appends, then
    /// the timers that are due. Firing timers first, as this did, handed a
    /// race to the timer whenever both were ready in one tick: uvicorn's
    /// keep-alive timeout closed a connection whose next request had
    /// already arrived, where asyncio (and uvloop) deliver the request and
    /// its `data_received` cancels the timer.
    pub fn take_expired(&mut self, out: &mut VecDeque<T>) -> usize {
        let n = self.expired.len();
        if n > 0 {
            out.extend(self.expired.drain(..));
        }
        n
    }

    /// R-030: 0 when ready callbacks or expired timers are pending; else
    /// time to the next timer deadline; clamped to [0, 100ms].
    pub fn poll_timeout(&mut self) -> Duration {
        self.deadline_bound = false;
        if !self.ready.is_empty() || !self.expired.is_empty() {
            return Duration::ZERO;
        }
        match self.timers.next_deadline() {
            Some(d) => {
                let now = self.clock.cached();
                if d <= now {
                    Duration::ZERO
                } else {
                    let left = Duration::from_nanos(d - now);
                    // Past the clamp, the park is an idle one: the timer is
                    // met by a later park that does end at its deadline.
                    self.deadline_bound = left <= MAX_PARK;
                    left.min(MAX_PARK)
                }
            }
            None => MAX_PARK,
        }
    }

    /// Poll the backend. MUST be called with the GIL released (R-021).
    /// Spin-then-park (R-060): busy-poll with zero timeout for `spin_us`
    /// before parking, unless the timeout is already zero.
    pub fn poll(&mut self, timeout: Duration) -> io::Result<()> {
        self.completions.clear();
        if timeout.is_zero() {
            // Full poll elision: with no op in flight, no readiness watch
            // armed and no completion staged, the kernel has nothing to
            // say at all -- pure-scheduling workloads (task ping-pong,
            // call_soon chains) skip the epoll_wait/GQCSEx syscall
            // entirely, not merely 249 ticks in 250. Cross-thread wakeups
            // are unaffected: their queue is drained in-memory every tick,
            // and the wake signal exists to interrupt PARKED polls, which
            // are never elided.
            if !self.backend.has_io_interest() {
                self.stats.polls_elided += 1;
                return Ok(());
            }
            // Poll-skip window (adopted from rloop): with ready callbacks
            // pending, an actual kernel poll is only taken every 250us —
            // bounded I/O-discovery staleness under CPU saturation in
            // exchange for skipping the epoll_wait/GQCSEx syscall on the
            // vast majority of busy ticks. Parked polls (timeout > 0) are
            // never skipped, so idle latency is unaffected.
            let now = self.clock.cached();
            if now.saturating_sub(self.last_poll_ns) < POLL_SKIP_NS {
                // Inside the skip window only the kernel round trip is
                // saved. Completions the backend already holds in user
                // space (inline successes, staged cancellations) cost no
                // syscall to deliver, and holding them back delayed a
                // finished recv or send by up to the whole window on
                // exactly the busy ticks where latency matters most.
                self.backend.take_staged(&mut self.completions);
                return Ok(());
            }
            self.stats.polls += 1;
            self.backend.try_poll(&mut self.completions)?;
            self.last_poll_ns = now;
            return Ok(());
        }
        self.stats.polls += 1;
        let mut timeout = timeout;
        if self.cfg.spin_us > 0 && (!self.cfg.adaptive_spin || self.spin_gate.should_spin()) {
            self.stats.spins += 1;
            let window = Duration::from_micros(self.cfg.spin_us);
            let spin_budget = window.min(timeout);
            let spin_start = std::time::Instant::now();
            let mut first_look = true;
            while spin_start.elapsed() < spin_budget {
                if self.backend.try_poll(&mut self.completions)? > 0 || !self.xthread.is_empty() {
                    // Work already waiting at the first look would have
                    // ended a park as fast; only work that arrived while
                    // spinning shows the spin paid (`SpinGate`).
                    if !first_look {
                        self.stats.spin_hits += 1;
                        self.spin_gate.record(true);
                    }
                    return Ok(());
                }
                first_look = false;
                std::hint::spin_loop();
            }
            // A spin a due timer cut short says nothing about the traffic.
            if spin_budget == window {
                self.spin_gate.record(false);
            }
            // The spin was part of the wait: a park that ends on time must
            // not add it again.
            timeout = timeout.saturating_sub(spin_start.elapsed());
        }
        let precise = self.deadline_bound && self.backend.precise_parks();
        if precise {
            self.stats.precise_parks += 1;
        }
        self.backend.poll_precise(&mut self.completions, Some(timeout), self.deadline_bound)?;
        self.last_poll_ns = self.clock.now_uncached();
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
            // Time passed and producers may have posted: a new tick
            // reading (taken lazily), absorb, drain and fire so this tick
            // dispatches them.
            self.clock.invalidate();
            self.absorb_cancellations();
            self.drain_xthread();
            self.fire_expired_timers();
            self.snapshot_batch();
        }
        // A non-blocking poll changes none of the inputs: the clock is not
        // refreshed (so no further timer can be due), nothing ran Python
        // (so nothing was cancelled), and a cross-thread item that raced
        // in since prepare_tick raised its own wakeup and is drained at the
        // top of the next tick. Re-running all three -- an atomic, a queue
        // length probe and a heap peek -- on every busy tick bought
        // nothing, and prepare_tick's batch snapshot is still exact.
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

    /// Lend the spare dispatch buffer to a run (see `take_batch`).
    ///
    /// The ready queue and this buffer trade places every tick. Keeping the
    /// one a run ends with, instead of dropping it with the run, is what
    /// lets both stop allocating once they reach the workload's largest
    /// batch: a fresh buffer per run regrew to that size every time, from
    /// freshly faulted memory -- 12% of a 50k-callback burst's time.
    pub fn take_spare(&mut self) -> VecDeque<T> {
        std::mem::take(&mut self.spare)
    }

    /// Take back the buffer `take_spare` lent, which must be empty: the
    /// larger of it and whatever is held now is kept.
    pub fn put_spare(&mut self, buf: VecDeque<T>) {
        debug_assert!(buf.is_empty());
        if buf.is_empty() && buf.capacity() >= self.spare.capacity() {
            self.spare = buf;
        }
    }

    /// Move the whole current dispatch batch into `out`, which must be
    /// empty: what `pop_ready_batched` hands out one token at a time, in
    /// O(1).
    ///
    /// The batch is the queue's snapshot, so the queue's buffer itself is
    /// swapped out, and `out`'s -- empty, capacity intact -- takes its
    /// place for whatever the batch schedules. The two buffers alternate
    /// tick by tick and stop allocating once they reach the workload's
    /// largest batch. Copying token by token instead grew a second buffer
    /// to the snapshot's size: a 200k-callback burst or a
    /// call_soon_threadsafe flood paid a reallocation series and the page
    /// faults of fresh memory for it on top of touching every token twice.
    pub fn take_batch(&mut self, out: &mut VecDeque<T>) {
        let n = std::mem::take(&mut self.batch_left);
        if n == 0 {
            return;
        }
        self.ready.swap_with(out);
        // Anything queued behind the snapshot belongs to the next tick:
        // back to the queue front, in order.
        while out.len() > n {
            let Some(token) = out.pop_back() else { break };
            self.ready.push_front(token);
        }
        self.stats.callbacks_dispatched += out.len() as u64;
    }

    /// Pop the next token of the current dispatch batch (R-054: never more
    /// than were queued when the batch started, matching asyncio's
    /// `_run_once` snapshot semantics).
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

    /// Callbacks ready to run: the ready queue and the expired timers.
    pub fn ready_len(&self) -> usize {
        self.ready.len() + self.expired.len()
    }

    /// Every token the reactor holds in its ready queue, expired timers
    /// and timer heap. (Cross-thread items in flight are not reachable
    /// without draining.)
    pub fn pending_iter(&self) -> impl Iterator<Item = &T> {
        self.ready.iter().chain(self.expired.iter()).chain(self.timers.iter())
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
        out.extend(self.expired.drain(..));
        let mut scratch = ReadyQueue::new();
        let timers = &mut self.timers;
        self.xthread.drain_into(&mut scratch, |deadline, token| timers.schedule(deadline, token));
        while let Some(t) = scratch.pop() {
            out.push(t);
        }
        // drain_all marks every timer unscheduled -- the cross-thread ones
        // just moved into the heap included.
        out.extend(self.timers.drain_all());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::spin_gate::{MAX_CREDIT, PROBE_EVERY};
    use crate::timer::testing::Tok;

    type T = Tok<u32>;

    fn tok(v: u32) -> T {
        Tok::new(v)
    }

    fn reactor() -> Reactor<T> {
        Reactor::new(ReactorConfig { spin_us: 0, ..Default::default() }).unwrap()
    }

    fn run_tick(r: &mut Reactor<T>) -> Vec<u32> {
        r.prepare_tick();
        let t = r.poll_timeout();
        r.poll(t).unwrap();
        r.finish_poll();
        let mut out = Vec::new();
        while let Some(tok) = r.pop_ready_batched() {
            out.push(tok.value);
        }
        let mut timers = VecDeque::new();
        r.take_expired(&mut timers);
        out.extend(timers.into_iter().map(|t| t.value));
        out
    }

    #[test]
    fn expired_timers_run_after_this_ticks_io_callbacks() {
        // asyncio's _run_once order: the previous iteration's callbacks,
        // then the I/O callbacks this poll reported, then due timers.
        let mut r = reactor();
        let now = r.now_fresh();
        r.schedule_timer(now, tok(3));
        r.push_ready(tok(1));
        r.prepare_tick();
        assert_eq!(r.poll_timeout(), Duration::ZERO, "an expired timer is ready work");
        r.poll(Duration::ZERO).unwrap();
        r.finish_poll_after(false);
        r.push_ready(tok(2)); // a readiness watch the poll reported
        r.absorb_into_batch();
        let mut batch = VecDeque::new();
        r.take_batch(&mut batch);
        let timers = r.take_expired(&mut batch);
        assert_eq!(timers, 1);
        assert_eq!(batch.into_iter().map(|t| t.value).collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(r.ready_len(), 0);
    }

    #[test]
    fn unpopped_expired_timers_keep_their_place() {
        let mut r = reactor();
        let now = r.now_fresh();
        r.schedule_timer(now, tok(1));
        r.schedule_timer(now, tok(2));
        r.prepare_tick();
        let mut batch = VecDeque::new();
        r.take_batch(&mut batch);
        assert_eq!(r.take_expired(&mut batch), 2);
        // An unwind before either ran: both go back, in order, and a
        // timer expiring later queues behind them.
        for t in batch.drain(..).rev() {
            r.unpop_expired(t);
        }
        r.schedule_timer(now, tok(3));
        assert_eq!(r.ready_len(), 2);
        assert_eq!(run_tick(&mut r), vec![1, 2, 3]);
    }

    #[test]
    fn call_soon_order_preserved() {
        let mut r = reactor();
        for i in 0..5 {
            r.push_ready(tok(i));
        }
        assert_eq!(run_tick(&mut r), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn timers_fire_in_order_after_deadline() {
        let mut r = reactor();
        let now = r.now_fresh();
        r.schedule_timer(now + 5_000_000, tok(2)); // +5ms
        r.schedule_timer(now + 1_000_000, tok(1)); // +1ms
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(run_tick(&mut r), vec![1, 2]);
    }

    #[test]
    fn cancelled_timer_never_fires() {
        let mut r = reactor();
        let now = r.now_fresh();
        let t1 = tok(1);
        r.schedule_timer(now + 1_000_000, t1.clone());
        r.schedule_timer(now + 2_000_000, tok(2));
        assert!(t1.cancel());
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
        r.schedule_timer(now + 3_000_000, tok(1));
        let t = r.poll_timeout();
        assert!(t <= Duration::from_millis(3));
        assert!(t > Duration::ZERO);
        r.push_ready(tok(9));
        assert_eq!(r.poll_timeout(), Duration::ZERO, "ready work forces zero timeout");
    }

    #[test]
    fn threadsafe_wakeup_delivers_token() {
        let mut r = reactor();
        let (q, wake) = r.cross_thread_handles();
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            if q.push(tok(42)) {
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
            out.push(tok.value);
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
            r.push_ready(tok(i));
        }
        r.prepare_tick();
        r.poll(Duration::ZERO).unwrap();
        r.finish_poll();
        let first = r.pop_ready_batched().unwrap();
        assert_eq!(first.value, 0);
        // Simulate a callback scheduling more work mid-dispatch.
        r.push_ready(tok(99));
        let mut rest = Vec::new();
        while let Some(tok) = r.pop_ready_batched() {
            rest.push(tok.value);
        }
        assert_eq!(rest, vec![1, 2], "token 99 must wait for the next tick");
        assert_eq!(run_tick(&mut r), vec![99]);
    }

    #[test]
    fn dispatch_batch_is_the_whole_snapshot() {
        let mut r = reactor();
        for i in 0..300 {
            r.push_ready(tok(i));
        }
        assert_eq!(run_tick(&mut r), (0..300).collect::<Vec<_>>());
        assert!(run_tick(&mut r).is_empty());
    }

    #[test]
    fn absorbed_pushes_join_the_current_batch() {
        let mut r = reactor();
        r.push_ready(tok(1));
        r.prepare_tick();
        r.poll(Duration::ZERO).unwrap();
        r.finish_poll();
        r.push_ready(tok(2)); // e.g. a readiness watch reported by the poll
        r.absorb_into_batch();
        let mut out = Vec::new();
        while let Some(tok) = r.pop_ready_batched() {
            out.push(tok.value);
            r.push_ready(tok.clone()); // scheduled during dispatch: next tick
        }
        assert_eq!(out, vec![1, 2]);
        assert_eq!(run_tick(&mut r), vec![1, 2]);
    }

    #[test]
    fn clear_pending_empties_everything() {
        let mut r = reactor();
        r.push_ready(tok(1));
        let now = r.now_fresh();
        r.schedule_timer(now + 1_000_000_000, tok(2));
        let (q, _w) = r.cross_thread_handles();
        let _ = q.push(tok(3));
        let mut drained: Vec<u32> = r.clear_pending().into_iter().map(|t| t.value).collect();
        drained.sort();
        assert_eq!(drained, vec![1, 2, 3]);
        assert_eq!(r.ready_len(), 0);
        assert_eq!(r.timers_len(), 0);
        assert!(run_tick(&mut r).is_empty());
    }

    fn spinning(adaptive_spin: bool) -> Reactor<T> {
        Reactor::new(ReactorConfig { spin_us: 20, adaptive_spin, ..Default::default() }).unwrap()
    }

    /// A park nothing ends early: its spin, if any, finds nothing.
    fn quiet_park(r: &mut Reactor<T>) {
        r.prepare_tick();
        r.poll(Duration::from_micros(300)).unwrap();
        r.finish_poll();
    }

    #[test]
    fn adaptive_spin_stops_spinning_while_nothing_arrives() {
        let mut fixed = spinning(false);
        let mut adaptive = spinning(true);
        for _ in 0..128 {
            quiet_park(&mut fixed);
            quiet_park(&mut adaptive);
        }
        assert_eq!(fixed.stats.spins, 128);
        // The misses spend the gate's credit; after that, a probe now and
        // then.
        let most = (MAX_CREDIT + 128 / PROBE_EVERY) as u64;
        assert!(adaptive.stats.spins <= most, "{} of 128 parks spun", adaptive.stats.spins);
        assert_eq!(adaptive.stats.polls, 128, "every park still polls");
    }

    #[test]
    fn work_waiting_before_the_spin_does_not_hold_the_gate_open() {
        // An echo server's parks: every other one finds the completion of
        // the send it just made already waiting, the rest wait out their
        // whole window. Counting the first as hits kept the gate open for
        // good, and it spun before every park.
        let mut r = spinning(true);
        let (q, wake) = r.cross_thread_handles();
        for i in 0..192 {
            r.prepare_tick();
            if i % 2 == 0 {
                if q.push(tok(i)) {
                    wake.wake();
                }
                r.poll(Duration::from_millis(1)).unwrap();
            } else {
                r.poll(Duration::from_micros(300)).unwrap();
            }
            r.finish_poll();
            while r.pop_ready_batched().is_some() {}
        }
        assert!(r.stats.spins < 192 / 3, "{} of 192 parks spun", r.stats.spins);
    }

    #[test]
    fn a_spin_a_due_timer_cuts_short_is_not_counted_against_the_gate() {
        // Parks shorter than the window say nothing about the traffic: a
        // loop running a fast timer keeps spinning through them.
        let mut r = spinning(true);
        for _ in 0..64 {
            r.prepare_tick();
            r.poll(Duration::from_micros(10)).unwrap();
            r.finish_poll();
        }
        assert_eq!(r.stats.spins, 64);
    }
}
