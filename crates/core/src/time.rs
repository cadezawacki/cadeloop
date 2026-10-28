//! The loop's monotonic clock (R-061).
//!
//! Ticks are nanoseconds on the clock behind Python's `time.monotonic()`,
//! so `loop.time()` and `time.monotonic()` agree the way they do under
//! asyncio and uvloop: deadlines computed from either one (`call_at`,
//! `asyncio.timeout_at`) mean the same instant. On Linux that is
//! `CLOCK_MONOTONIC` itself (a vDSO read, no syscall). Elsewhere the
//! platform reading is `Instant`-based (QueryPerformanceCounter on
//! Windows) and the binding layer aligns it to Python's clock once, at
//! loop creation, through [`Clock::with_offset`].
//!
//! The reactor keeps one reading per tick ([`Clock::cached`]) for its own
//! bookkeeping -- timer expiry, poll timeouts, connection deadlines --
//! where per-tick granularity is the asyncio contract (`_run_once` reads
//! the clock once to decide which timers are due). That reading is taken
//! lazily, on the tick's first use: a tick with no timer pending and no
//! I/O to time has no use for the clock at all, and the read it used to
//! pay regardless was ~4% of a pure-scheduling tick (task steps, future
//! chains). Anything user-visible
//! reads the clock fresh ([`Clock::now`]): `loop.time()`, and the base of
//! every `call_later` deadline. A per-tick value there was frozen for the
//! whole tick: after a callback that blocked for 200ms,
//! `asyncio.sleep(0.1)` returned at once, and `loop.time()` deltas
//! measured inside a callback were 0.

use std::cell::Cell;

/// Nanoseconds on the loop clock (see the module docs).
pub type Ticks = u64;

/// One read of the platform's monotonic clock, in nanoseconds.
#[inline]
pub fn raw_now() -> Ticks {
    #[cfg(target_os = "linux")]
    {
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        // SAFETY: valid out-pointer; CLOCK_MONOTONIC always exists on Linux.
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
        (ts.tv_sec as u64).wrapping_mul(1_000_000_000).wrapping_add(ts.tv_nsec as u64)
    }
    #[cfg(not(target_os = "linux"))]
    {
        use std::sync::OnceLock;
        use std::time::Instant;
        static EPOCH: OnceLock<Instant> = OnceLock::new();
        EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as Ticks
    }
}

/// Whether [`raw_now`] already reads the clock behind `time.monotonic()`,
/// so no alignment offset is needed.
pub const RAW_IS_PYTHON_MONOTONIC: bool = cfg!(target_os = "linux");

#[derive(Debug)]
pub struct Clock {
    /// Added to every platform reading (0 when it is Python's clock).
    offset: i64,
    cached: Cell<Ticks>,
    /// The tick moved on since `cached` was read: re-read on next use.
    stale: Cell<bool>,
}

impl Clock {
    pub fn new() -> Self {
        Self::with_offset(0)
    }

    /// A clock reading `raw_now() + offset`, e.g. aligned to another
    /// monotonic clock by `offset = other_now - raw_now()`.
    pub fn with_offset(offset: i64) -> Self {
        let c = Clock { offset, cached: Cell::new(0), stale: Cell::new(true) };
        c.refresh();
        c
    }

    /// A fresh reading; leaves the per-tick cache alone.
    #[inline]
    pub fn now(&self) -> Ticks {
        raw_now().saturating_add_signed(self.offset)
    }

    /// Read the clock now and make that the per-tick reading.
    #[inline]
    pub fn refresh(&self) -> Ticks {
        let t = self.now();
        self.cached.set(t);
        self.stale.set(false);
        t
    }

    /// A new tick began: the next [`Clock::cached`] reads the clock.
    #[inline]
    pub fn invalidate(&self) {
        self.stale.set(true);
    }

    /// The reactor's per-tick time: read on the tick's first use, then
    /// the same value until the next [`Clock::invalidate`].
    #[inline]
    pub fn cached(&self) -> Ticks {
        if self.stale.get() {
            self.refresh()
        } else {
            self.cached.get()
        }
    }

    /// Alias of [`Clock::now`], kept for the reactor's call sites.
    #[inline]
    pub fn now_uncached(&self) -> Ticks {
        self.now()
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

pub const NANOS_PER_SEC: u64 = 1_000_000_000;

#[inline]
pub fn ticks_to_secs_f64(t: Ticks) -> f64 {
    t as f64 / NANOS_PER_SEC as f64
}

#[inline]
pub fn secs_f64_to_ticks(s: f64) -> Ticks {
    if s <= 0.0 || s.is_nan() {
        return 0;
    }
    // Saturate rather than wrap for absurd deadlines.
    let ns = s * NANOS_PER_SEC as f64;
    if ns >= u64::MAX as f64 {
        u64::MAX
    } else {
        ns as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_is_read_lazily_after_invalidate() {
        let c = Clock::new();
        let a = c.cached();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert_eq!(c.cached(), a, "stable until invalidated");
        c.invalidate();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = c.cached();
        assert!(b >= a + 4_000_000, "read on first use after the invalidate, not at it");
        assert_eq!(c.cached(), b);
    }

    #[test]
    fn cached_is_stable_between_refreshes() {
        let c = Clock::new();
        let a = c.refresh();
        let b = c.cached();
        let d = c.cached();
        assert_eq!(a, b);
        assert_eq!(b, d);
    }

    #[test]
    fn refresh_is_monotonic() {
        let c = Clock::new();
        let a = c.refresh();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = c.refresh();
        assert!(b > a);
    }

    #[test]
    fn now_is_fresh_and_leaves_the_cache() {
        let c = Clock::new();
        let a = c.refresh();
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(c.now() >= a + 2_000_000);
        assert_eq!(c.cached(), a);
    }

    #[test]
    fn offset_shifts_readings() {
        let base = Clock::new();
        let ahead = Clock::with_offset(5_000_000_000);
        let (x, y) = (base.now(), ahead.now());
        assert!(y >= x + 5_000_000_000 && y < x + 6_000_000_000);
        // A negative offset larger than the reading saturates at 0.
        assert_eq!(Clock::with_offset(i64::MIN).now(), 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_clock_is_clock_monotonic() {
        // Same clock as Python's time.monotonic(): readings bracket one
        // taken straight from the kernel interface.
        let a = raw_now();
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
        let mid = ts.tv_sec as u64 * NANOS_PER_SEC + ts.tv_nsec as u64;
        let b = raw_now();
        assert!(a <= mid && mid <= b);
    }

    #[test]
    fn secs_roundtrip() {
        let t = secs_f64_to_ticks(1.5);
        assert_eq!(t, 1_500_000_000);
        assert!((ticks_to_secs_f64(t) - 1.5).abs() < 1e-9);
        assert_eq!(secs_f64_to_ticks(-1.0), 0);
        assert_eq!(secs_f64_to_ticks(f64::NAN), 0);
        assert_eq!(secs_f64_to_ticks(f64::INFINITY), u64::MAX);
    }
}
