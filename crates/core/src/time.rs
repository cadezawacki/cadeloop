//! Monotonic clock with per-tick caching (R-061).
//!
//! On Windows, `std::time::Instant` is implemented with
//! `QueryPerformanceCounter`, which satisfies R-061's clock-source
//! requirement without a direct Win32 call; the conversion to nanoseconds is
//! done once per read. `loop.time()` at the Python layer returns the value
//! cached at the top of the current tick — reduced granularity within a tick
//! is documented behavior.

use std::time::Instant;

/// Nanoseconds since the loop's epoch (loop creation).
pub type Ticks = u64;

#[derive(Debug)]
pub struct Clock {
    epoch: Instant,
    cached: Ticks,
}

impl Clock {
    pub fn new() -> Self {
        let epoch = Instant::now();
        Clock { epoch, cached: 0 }
    }

    /// Read the hardware clock and refresh the per-tick cache.
    /// Called at least once per reactor tick.
    pub fn refresh(&mut self) -> Ticks {
        self.cached = self.epoch.elapsed().as_nanos() as Ticks;
        self.cached
    }

    /// The value cached by the last `refresh()`. This is what
    /// `loop.time()` reports (R-061).
    #[inline]
    pub fn cached(&self) -> Ticks {
        self.cached
    }

    /// An uncached read, for callers that need a fresh timestamp without
    /// touching the tick cache (e.g. computing poll timeouts).
    #[inline]
    pub fn now_uncached(&self) -> Ticks {
        self.epoch.elapsed().as_nanos() as Ticks
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
    if s <= 0.0 {
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
    fn cached_is_stable_between_refreshes() {
        let mut c = Clock::new();
        let a = c.refresh();
        let b = c.cached();
        let d = c.cached();
        assert_eq!(a, b);
        assert_eq!(b, d);
    }

    #[test]
    fn refresh_is_monotonic() {
        let mut c = Clock::new();
        let a = c.refresh();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = c.refresh();
        assert!(b > a);
    }

    #[test]
    fn secs_roundtrip() {
        let t = secs_f64_to_ticks(1.5);
        assert_eq!(t, 1_500_000_000);
        assert!((ticks_to_secs_f64(t) - 1.5).abs() < 1e-9);
        assert_eq!(secs_f64_to_ticks(-1.0), 0);
    }
}
