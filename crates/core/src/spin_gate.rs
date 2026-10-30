//! Adaptive spin-then-park (R-060, `ReactorConfig::adaptive_spin`).
//!
//! A spin pays for itself only when work arrives inside its window: then
//! the loop never makes the blocking wait, nor pays the wake-up after it.
//! When nothing arrives it is the whole window of CPU burned before the
//! park it delays -- on Windows a `GetQueuedCompletionStatusEx` per turn of
//! the spin. Which of the two a park gets is decided by the traffic, not
//! the configuration: a client pinging back to back lands nearly every
//! time, one sending every 50 us never lands in a 20 us window, and a
//! production server's counters showed 22,486 spins that found work (some
//! of it already waiting) beside 383,677 that found nothing and parked.
//!
//! The gate keeps a credit: a spin that catches work earns `HIT_CREDIT`, one
//! that catches nothing spends one, and the loop spins while credit is
//! left. So it keeps spinning while more than one spin in `HIT_CREDIT + 1`
//! catches work, and stops below that. While it is shut, one park in
//! `PROBE_EVERY` spins anyway, so traffic that turns dense again reopens it
//! on its first catch. All of it is a few integer operations per park,
//! against a spin window measured in microseconds.
//!
//! A credit rather than a run of misses: the first version shut after four
//! misses in a row, and a ping-pong client whose spins caught 97% of its
//! messages still shut it whenever a hiccup bunched four misses together.
//! Shut, the gate changed the traffic it was measuring: its spins caught
//! only about 60% of messages, and runs of misses came often enough to
//! keep it shut. One of three measured runs stayed there, with more than
//! half its parks not spinning. A credit closes on the hit rate, not on how
//! the misses bunch.
//!
//! Only work that ARRIVES during a spin counts as a catch. Work already
//! waiting at its first look -- typically the completion of a send the
//! loop just made -- would have ended a park just as fast, so it says
//! nothing either way; counted as a catch, it kept the gate open for an
//! echo server whose every other spin found its own send completion while
//! the rest all missed.

/// Credit a spin that catches work earns; one that catches nothing spends
/// one. The gate stays open while more than one spin in `HIT_CREDIT + 1`
/// catches work.
pub(crate) const HIT_CREDIT: u8 = 3;

/// The most credit the gate holds: the run of misses that shuts it after
/// dense traffic.
pub(crate) const MAX_CREDIT: u8 = 12;

/// While shut, one park in this many spins: what a quiet loop still pays,
/// as a fraction of the fixed spin's cost, to notice dense traffic within
/// a few parks of its return.
pub(crate) const PROBE_EVERY: u8 = 8;

#[derive(Debug)]
pub(crate) struct SpinGate {
    credit: u8,
    /// Parks since the last probe while shut.
    since_probe: u8,
}

impl SpinGate {
    pub(crate) fn new() -> Self {
        SpinGate { credit: MAX_CREDIT, since_probe: 0 }
    }

    /// Spin before this park?
    pub(crate) fn should_spin(&mut self) -> bool {
        if self.credit > 0 || self.since_probe + 1 >= PROBE_EVERY {
            // Open, or a probe is due -- and stays due until a spin
            // actually tells the gate something (`record`).
            return true;
        }
        self.since_probe += 1;
        false
    }

    /// What a spin found: work that arrived during it (`hit`), or nothing
    /// in its whole window. A spin ended early, by work already waiting or
    /// by a due timer, is not recorded.
    pub(crate) fn record(&mut self, hit: bool) {
        self.since_probe = 0;
        self.credit =
            if hit { (self.credit + HIT_CREDIT).min(MAX_CREDIT) } else { self.credit.saturating_sub(1) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parks out of `n` that spin, starting from `gate`, when every spin
    /// finds work with probability `p` (a fixed-seed generator).
    fn spins_from(gate: &mut SpinGate, p: f64, n: usize) -> usize {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut spun = 0;
        for _ in 0..n {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let hit = (seed % 1_000_000) as f64 / 1e6 < p;
            if gate.should_spin() {
                spun += 1;
                gate.record(hit);
            }
        }
        spun
    }

    fn spins_for(p: f64, n: usize) -> usize {
        spins_from(&mut SpinGate::new(), p, n)
    }

    /// A gate that a run of misses has just shut.
    fn shut() -> SpinGate {
        let mut gate = SpinGate::new();
        for _ in 0..MAX_CREDIT {
            assert!(gate.should_spin());
            gate.record(false);
        }
        gate
    }

    #[test]
    fn dense_traffic_keeps_every_spin() {
        assert_eq!(spins_for(1.0, 10_000), 10_000);
        assert!(spins_for(0.6, 10_000) > 9_900, "a 60% hit rate keeps the gate open");
    }

    #[test]
    fn misses_bunched_in_dense_traffic_do_not_shut_the_gate() {
        // Each round: a run of misses one short of the limit, then four
        // catches. Eleven spins in fifteen miss, yet no park goes unspun.
        let mut gate = SpinGate::new();
        for _ in 0..100 {
            for hit in std::iter::repeat_n(false, MAX_CREDIT as usize - 1).chain([true; 4]) {
                assert!(gate.should_spin());
                gate.record(hit);
            }
        }
    }

    #[test]
    fn sparse_traffic_spins_about_one_park_in_probe_every() {
        let n = 80_000;
        let spun = spins_for(0.0, n);
        assert!(spun <= n / PROBE_EVERY as usize + MAX_CREDIT as usize, "{spun} of {n}");
        // A production server's rate: ~5% of fixed spins found work.
        let spun = spins_for(0.055, n);
        assert!(spun < n / 5, "{spun} of {n} parks spun at a 5.5% hit rate");
    }

    #[test]
    fn a_probe_that_learns_nothing_stays_due() {
        let mut gate = shut();
        let mut parks = 0;
        while !gate.should_spin() {
            parks += 1;
        }
        assert_eq!(parks, PROBE_EVERY as usize - 1);
        // The probe found work already waiting: nothing recorded, so the
        // next park probes again rather than waiting another round.
        assert!(gate.should_spin());
        gate.record(false);
        assert!(!gate.should_spin());
    }

    #[test]
    fn traffic_turning_dense_reopens_the_gate_within_a_probe() {
        let mut gate = shut();
        let mut parks = 0;
        loop {
            parks += 1;
            if gate.should_spin() {
                gate.record(true);
                break;
            }
        }
        assert!(parks <= PROBE_EVERY as usize);
        assert!((0..100).all(|_| {
            let spin = gate.should_spin();
            gate.record(true);
            spin
        }));
        // ... and a gate shut while spinning still paid -- the trap a
        // run-of-misses rule fell into -- comes back for good.
        let spun = spins_from(&mut shut(), 0.6, 10_000);
        assert!(spun > 9_800, "{spun} of 10000 parks spun at a 60% hit rate");
    }
}
