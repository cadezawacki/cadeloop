//! Platform-independent bookkeeping for the RIO backend (R-040..R-044).
//!
//! Everything in this module is pure Rust with no Windows dependency, so
//! the logic that RIO correctness hinges on — buffer-pointer → registered
//! region resolution and send-staging slot accounting — is unit-tested on
//! every development platform. The `rio.rs` FFI glue stays thin and is
//! compile-verified via the `x86_64-pc-windows-msvc` cross-check + the
//! Windows CI build (behavioral validation is Windows-hardware work,
//! tracked in the M3 roadmap).

/// One registered buffer region: `[base, base+len)` known to the kernel
/// under `cookie` (a `RIO_BUFFERID` on Windows).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionEntry {
    pub base: usize,
    pub len: usize,
    pub cookie: u64,
}

/// Sorted registry of registered buffer regions (R-043). Posted buffers
/// arrive as raw pointers; RIO wants `(buffer id, offset, length)` — this
/// map is the reverse lookup, kept sorted for binary search.
#[derive(Default)]
pub struct RegionMap {
    regions: Vec<RegionEntry>,
}

impl RegionMap {
    pub fn new() -> Self {
        RegionMap { regions: Vec::new() }
    }

    /// Register a region. Regions must not overlap (they are distinct
    /// slab allocations); overlapping inserts are rejected.
    pub fn insert(&mut self, base: usize, len: usize, cookie: u64) -> bool {
        let at = self.regions.partition_point(|r| r.base < base);
        let overlaps_prev =
            at > 0 && self.regions[at - 1].base + self.regions[at - 1].len > base;
        let overlaps_next = self.regions.get(at).is_some_and(|r| base + len > r.base);
        if overlaps_prev || overlaps_next {
            return false;
        }
        self.regions.insert(at, RegionEntry { base, len, cookie });
        true
    }

    /// Drop a region by its cookie. Returns whether it existed.
    pub fn remove_cookie(&mut self, cookie: u64) -> bool {
        match self.regions.iter().position(|r| r.cookie == cookie) {
            Some(i) => {
                self.regions.remove(i);
                true
            }
            None => false,
        }
    }

    /// Resolve `[ptr, ptr+len)` to `(cookie, offset)` — the whole range
    /// must lie inside ONE registered region (RIO requests may not span
    /// registrations).
    pub fn resolve(&self, ptr: usize, len: usize) -> Option<(u64, u32)> {
        let at = self.regions.partition_point(|r| r.base <= ptr);
        if at == 0 {
            return None;
        }
        let r = &self.regions[at - 1];
        if ptr >= r.base && ptr + len <= r.base + r.len {
            Some((r.cookie, (ptr - r.base) as u32))
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.regions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }
}

/// Send-staging slot accounting (R-044): outgoing gather payloads are
/// copied into fixed-size slots carved out of registered regions (RIO
/// requests take exactly one registered buffer — arbitrary Python-owned
/// pointers cannot be posted directly). This ledger tracks slot indices;
/// the memory itself lives in the platform layer.
///
/// Slot ids encode `(region_index * slots_per_region) + slot_in_region`.
pub struct StagingLedger {
    slots_per_region: u32,
    free: Vec<u32>,
    regions: u32,
    in_use: u32,
}

impl StagingLedger {
    pub fn new(slots_per_region: u32) -> Self {
        assert!(slots_per_region > 0);
        StagingLedger { slots_per_region, free: Vec::new(), regions: 0, in_use: 0 }
    }

    /// Announce a newly created (registered) staging region; its slots
    /// join the freelist. Returns the region index.
    pub fn add_region(&mut self) -> u32 {
        let region = self.regions;
        self.regions += 1;
        let base = region * self.slots_per_region;
        // LIFO: hottest slots first.
        for i in (0..self.slots_per_region).rev() {
            self.free.push(base + i);
        }
        region
    }

    pub fn alloc(&mut self) -> Option<u32> {
        let id = self.free.pop()?;
        self.in_use += 1;
        Some(id)
    }

    pub fn free(&mut self, id: u32) {
        debug_assert!(id < self.regions * self.slots_per_region);
        debug_assert!(!self.free.contains(&id), "double free of staging slot {id}");
        self.in_use -= 1;
        self.free.push(id);
    }

    /// Decompose a slot id into (region index, slot index within region).
    pub fn locate(&self, id: u32) -> (u32, u32) {
        (id / self.slots_per_region, id % self.slots_per_region)
    }

    pub fn in_use(&self) -> u32 {
        self.in_use
    }

    pub fn regions(&self) -> u32 {
        self.regions
    }
}

/// CQ capacity accounting (R-041): RIO reserves completion-queue slots
/// per request queue AT RQ CREATION, so overflow is a creation-time
/// failure, not a runtime data-loss event. This tracks reservations and
/// decides growth (doubling, capped) before each RQ creation.
pub struct CqLedger {
    pub size: u32,
    pub reserved: u32,
    pub max_size: u32,
}

impl CqLedger {
    pub fn new(size: u32, max_size: u32) -> Self {
        CqLedger { size, reserved: 0, max_size: max_size.max(size) }
    }

    /// Reserve `n` slots for a new RQ. Returns `Ok(Some(new_size))` when
    /// the CQ must first grow to `new_size`, `Ok(None)` when capacity is
    /// already available, `Err(())` when even the cap cannot fit it.
    #[allow(clippy::result_unit_err)] // binary outcome; caller maps to io::Error
    pub fn plan_reserve(&self, n: u32) -> Result<Option<u32>, ()> {
        let needed = self.reserved.checked_add(n).ok_or(())?;
        if needed <= self.size {
            return Ok(None);
        }
        let mut new_size = self.size.max(1);
        while new_size < needed {
            new_size = new_size.saturating_mul(2).min(self.max_size);
            if new_size == self.max_size && new_size < needed {
                return Err(());
            }
        }
        Ok(Some(new_size))
    }

    pub fn commit(&mut self, n: u32, grown_to: Option<u32>) {
        if let Some(s) = grown_to {
            self.size = s;
        }
        self.reserved += n;
    }

    pub fn release(&mut self, n: u32) {
        self.reserved = self.reserved.saturating_sub(n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_resolution_and_boundaries() {
        let mut m = RegionMap::new();
        assert!(m.insert(0x1000, 0x1000, 7));
        assert!(m.insert(0x4000, 0x2000, 9));
        // interior
        assert_eq!(m.resolve(0x1800, 0x100), Some((7, 0x800)));
        // exact start / exact end
        assert_eq!(m.resolve(0x1000, 0x1000), Some((7, 0)));
        assert_eq!(m.resolve(0x1fff, 1), Some((7, 0xfff)));
        // spilling past the end of a region
        assert_eq!(m.resolve(0x1fff, 2), None);
        // gap between regions
        assert_eq!(m.resolve(0x2000, 1), None);
        assert_eq!(m.resolve(0x3fff, 1), None);
        // second region
        assert_eq!(m.resolve(0x5000, 0x1000), Some((9, 0x1000)));
        // before everything
        assert_eq!(m.resolve(0x0, 1), None);
    }

    #[test]
    fn region_overlap_rejected_and_removal() {
        let mut m = RegionMap::new();
        assert!(m.insert(0x1000, 0x1000, 1));
        assert!(!m.insert(0x1800, 0x1000, 2), "overlapping tail");
        assert!(!m.insert(0x800, 0x1000, 3), "overlapping head");
        assert!(m.insert(0x2000, 0x1000, 4), "adjacent is fine");
        assert!(m.remove_cookie(1));
        assert!(!m.remove_cookie(1));
        assert_eq!(m.resolve(0x1800, 8), None);
        assert_eq!(m.resolve(0x2800, 8), Some((4, 0x800)));
    }

    #[test]
    fn staging_ledger_recycles_lifo() {
        let mut l = StagingLedger::new(4);
        assert_eq!(l.alloc(), None, "no regions yet");
        assert_eq!(l.add_region(), 0);
        let a = l.alloc().unwrap();
        let b = l.alloc().unwrap();
        assert_eq!((a, b), (0, 1));
        assert_eq!(l.locate(a), (0, 0));
        l.free(a);
        assert_eq!(l.alloc().unwrap(), a, "LIFO reuse");
        assert_eq!(l.in_use(), 2);
        assert_eq!(l.add_region(), 1);
        // drain everything: 4 remaining of region 0 minus the two held...
        let mut got = Vec::new();
        while let Some(s) = l.alloc() {
            got.push(s);
        }
        assert_eq!(got.len(), 6, "2 left in region 0 + 4 in region 1");
        assert_eq!(l.in_use(), 8);
        assert!(got.iter().any(|&s| l.locate(s).0 == 1));
    }

    #[test]
    fn cq_ledger_growth_and_cap() {
        let mut c = CqLedger::new(64, 256);
        assert_eq!(c.plan_reserve(64), Ok(None));
        c.commit(64, None);
        // next reservation needs growth
        assert_eq!(c.plan_reserve(1), Ok(Some(128)));
        c.commit(64, Some(128));
        assert_eq!(c.size, 128);
        assert_eq!(c.reserved, 128);
        // grow straight to the cap
        assert_eq!(c.plan_reserve(128), Ok(Some(256)));
        c.commit(128, Some(256));
        // beyond the cap: refused
        assert_eq!(c.plan_reserve(1), Err(()));
        c.release(128);
        assert_eq!(c.plan_reserve(64), Ok(None));
    }
}
