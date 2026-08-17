//! Buffer slabs (R-071, R-072, R-073).
//!
//! Size classes {4 KiB, 16 KiB, 64 KiB}. Each slab is one 2 MiB region
//! (`VirtualAlloc` on Windows, attempting large pages via
//! `MEM_LARGE_PAGES` when `SeLockMemoryPrivilege` is held, with silent
//! fallback; `alloc_zeroed` elsewhere for dev/test builds), carved into
//! fixed-size slots with a per-class freelist.
//!
//! The pool is thread-affine — only the loop thread touches it — so slot
//! refcounts are plain `u32`s, no atomics, no locks (R-071).
//!
//! Slot lifetime (R-073): every slot carries a refcount. Kernel ops hold a
//! reference while the buffer is posted; the binding layer holds one for
//! each exported `memoryview` (released from the Python buffer-release
//! callback via `gil_boundary`). A slot returns to the freelist only when
//! the count hits zero. Debug builds poison freed slots with `0xDD`.
//!
//! RIO (R-043): each region is registered with `RIORegisterBuffer` exactly
//! once at slab creation; `regions()` exposes (ptr, len) pairs plus a
//! registration cookie slot for the RIO backend to fill.

use std::fmt;

pub const CLASS_SIZES: [usize; 3] = [4 * 1024, 16 * 1024, 64 * 1024];
pub const REGION_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SizeClass {
    S4K = 0,
    S16K = 1,
    S64K = 2,
}

impl SizeClass {
    #[inline]
    pub fn size(self) -> usize {
        CLASS_SIZES[self as usize]
    }

    /// Smallest class that fits `n` bytes, or None if > 64 KiB (callers
    /// chunk at the largest class).
    pub fn fitting(n: usize) -> Option<SizeClass> {
        match n {
            0..=4096 => Some(SizeClass::S4K),
            4097..=16384 => Some(SizeClass::S16K),
            16385..=65536 => Some(SizeClass::S64K),
            _ => None,
        }
    }

    pub fn slots_per_region(self) -> usize {
        REGION_SIZE / self.size()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotId {
    pub class: SizeClass,
    /// Global slot index within the class: region * slots_per_region + slot.
    pub index: u32,
    /// Bumped every time the slot is recycled, so an id kept past its
    /// release stops resolving (R-073).
    ///
    /// Without this a stale id is indistinguishable from a live one: the
    /// index is still in range and the slot is very likely re-acquired,
    /// so `release()` on it would decrement -- and quite possibly free --
    /// a buffer that now belongs to a different connection, leaving two
    /// owners writing the same memory. That is the shape of the UAF the
    /// spec calls out as the top risk (16), reached from the buffer side
    /// rather than the OVERLAPPED side.
    pub generation: u32,
}

impl fmt::Display for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "buf[{:?}:{}#{}]", self.class, self.index, self.generation)
    }
}

/// One 2 MiB backing region.
struct Region {
    ptr: *mut u8,
    /// Filled by the RIO backend after RIORegisterBuffer (opaque here).
    pub rio_buffer_id: Option<u64>,
    large_pages: bool,
}

// Thread-affine by contract (loop thread only); pointers make this !Send by
// default which is exactly right. No unsafe Send/Sync impls.

impl Region {
    fn alloc() -> Region {
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::Memory::{
                VirtualAlloc, MEM_COMMIT, MEM_LARGE_PAGES, MEM_RESERVE, PAGE_READWRITE,
            };
            // R-071: attempt large pages, silent fallback.
            let ptr = unsafe {
                VirtualAlloc(
                    std::ptr::null(),
                    REGION_SIZE,
                    MEM_COMMIT | MEM_RESERVE | MEM_LARGE_PAGES,
                    PAGE_READWRITE,
                )
            };
            if !ptr.is_null() {
                return Region { ptr: ptr.cast(), rio_buffer_id: None, large_pages: true };
            }
            let ptr = unsafe {
                VirtualAlloc(std::ptr::null(), REGION_SIZE, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE)
            };
            assert!(!ptr.is_null(), "VirtualAlloc({REGION_SIZE}) failed");
            Region { ptr: ptr.cast(), rio_buffer_id: None, large_pages: false }
        }
        #[cfg(not(windows))]
        {
            let layout = std::alloc::Layout::from_size_align(REGION_SIZE, 4096).unwrap();
            let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!ptr.is_null(), "region alloc failed");
            Region { ptr, rio_buffer_id: None, large_pages: false }
        }
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        #[cfg(windows)]
        unsafe {
            use windows_sys::Win32::System::Memory::{VirtualFree, MEM_RELEASE};
            VirtualFree(self.ptr.cast(), 0, MEM_RELEASE);
        }
        #[cfg(not(windows))]
        unsafe {
            let layout = std::alloc::Layout::from_size_align(REGION_SIZE, 4096).unwrap();
            std::alloc::dealloc(self.ptr, layout);
        }
    }
}

struct ClassPool {
    class: SizeClass,
    regions: Vec<Region>,
    freelist: Vec<u32>,
    /// Refcount per allocated slot index; 0 == free.
    refcounts: Vec<u32>,
    /// Parallel to `refcounts`; see `SlotId::generation`.
    generations: Vec<u32>,
    /// `retain`/`release` calls refused because the id was stale or out of
    /// range. Always zero in a correct run.
    stale_rejections: u64,
    in_use: usize,
}

impl ClassPool {
    fn new(class: SizeClass) -> Self {
        ClassPool {
            class,
            regions: Vec::new(),
            freelist: Vec::new(),
            refcounts: Vec::new(),
            generations: Vec::new(),
            stale_rejections: 0,
            in_use: 0,
        }
    }

    fn grow(&mut self) {
        let region = Region::alloc();
        let base = (self.regions.len() * self.class.slots_per_region()) as u32;
        self.regions.push(region);
        let n = self.class.slots_per_region() as u32;
        self.refcounts.resize(self.refcounts.len() + n as usize, 0);
        self.generations.resize(self.generations.len() + n as usize, 0);
        for i in (0..n).rev() {
            self.freelist.push(base + i);
        }
    }

    /// Is this id both in range and current? Bounds are checked because an
    /// id can outlive the pool state it names, and an out-of-range index
    /// should resolve to "not live" rather than panic.
    fn live(&self, id: SlotId) -> bool {
        let i = id.index as usize;
        self.refcounts.get(i).is_some_and(|&rc| rc > 0) && self.generations.get(i) == Some(&id.generation)
    }

    fn slot_ptr(&self, index: u32) -> *mut u8 {
        let per = self.class.slots_per_region();
        let region = &self.regions[index as usize / per];
        let offset = (index as usize % per) * self.class.size();
        unsafe { region.ptr.add(offset) }
    }
}

pub struct BufferPool {
    classes: [ClassPool; 3],
    /// Set when a slab region was allocated since the last
    /// `take_regions_dirty` — the reactor's cue to run the backend's
    /// `register_buffers` hook (R-043; no-op on IOCP/epoll).
    regions_dirty: bool,
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferPool {
    pub fn new() -> Self {
        BufferPool {
            classes: [
                ClassPool::new(SizeClass::S4K),
                ClassPool::new(SizeClass::S16K),
                ClassPool::new(SizeClass::S64K),
            ],
            regions_dirty: false,
        }
    }

    #[inline]
    fn class(&self, c: SizeClass) -> &ClassPool {
        &self.classes[c as usize]
    }

    #[inline]
    fn class_mut(&mut self, c: SizeClass) -> &mut ClassPool {
        &mut self.classes[c as usize]
    }

    /// Acquire a slot with refcount 1.
    pub fn acquire(&mut self, class: SizeClass) -> SlotId {
        if self.class(class).freelist.is_empty() {
            self.regions_dirty = true;
        }
        let pool = self.class_mut(class);
        if pool.freelist.is_empty() {
            pool.grow();
        }
        let index = pool.freelist.pop().expect("freelist refilled by grow");
        debug_assert_eq!(pool.refcounts[index as usize], 0, "acquired slot has live refs");
        pool.refcounts[index as usize] = 1;
        pool.in_use += 1;
        SlotId { class, index, generation: pool.generations[index as usize] }
    }

    /// Add a reference (e.g. a memoryview export or a posted kernel op).
    pub fn retain(&mut self, id: SlotId) {
        let pool = self.class_mut(id.class);
        if !pool.live(id) {
            pool.stale_rejections += 1;
            return;
        }
        pool.refcounts[id.index as usize] += 1;
    }

    /// Drop a reference; slot returns to the freelist at zero (R-073).
    /// Returns true when the slot was actually freed.
    pub fn release(&mut self, id: SlotId) -> bool {
        let size = id.class.size();
        let pool = self.class_mut(id.class);
        if !pool.live(id) {
            // Refusing is the whole point of the generation: the index is
            // still in range and the slot has very likely been re-acquired
            // by someone else, so decrementing here could free a buffer
            // that is in active use by another connection.
            //
            // Counted rather than `debug_assert!`ed. An assertion would
            // make the detector fire only in debug builds, i.e. everywhere
            // except where it matters; a counter works in both profiles,
            // and `stats()["stale_buffer_ids"]` being non-zero in
            // production is an unambiguous ownership bug worth seeing.
            pool.stale_rejections += 1;
            return false;
        }
        let rc = &mut pool.refcounts[id.index as usize];
        *rc -= 1;
        if *rc != 0 {
            return false;
        }
        if cfg!(debug_assertions) {
            // R-073: poison freed slots.
            unsafe { std::ptr::write_bytes(pool.slot_ptr(id.index), 0xDD, size) };
        }
        // Recycled: every id handed out for this slot is now stale.
        pool.generations[id.index as usize] = pool.generations[id.index as usize].wrapping_add(1);
        pool.in_use -= 1;
        pool.freelist.push(id.index);
        true
    }

    pub fn refcount(&self, id: SlotId) -> u32 {
        let pool = self.class(id.class);
        if pool.live(id) {
            pool.refcounts[id.index as usize]
        } else {
            0
        }
    }

    /// Raw slot memory. Valid until the slot's refcount reaches zero; the
    /// slab region itself is pinned for the pool's lifetime.
    pub fn slot_ptr(&self, id: SlotId) -> *mut u8 {
        debug_assert!(self.refcount(id) > 0, "slot_ptr on free slot {id}");
        self.class(id.class).slot_ptr(id.index)
    }

    pub fn slot_len(&self, id: SlotId) -> usize {
        id.class.size()
    }

    /// Buffers currently held (for `loop.stats()`, R-103).
    pub fn in_use(&self) -> usize {
        self.classes.iter().map(|c| c.in_use).sum()
    }

    /// `retain`/`release` calls refused because the `SlotId` was stale or
    /// out of range. Non-zero means something held a buffer id past its
    /// release -- an ownership bug, and the near miss of a use-after-free.
    pub fn stale_rejections(&self) -> u64 {
        self.classes.iter().map(|c| c.stale_rejections).sum()
    }

    /// (ptr, len, rio_cookie) per region of a class, for one-time
    /// RIORegisterBuffer at slab creation (R-043).
    pub fn regions_mut(
        &mut self,
        class: SizeClass,
    ) -> impl Iterator<Item = (*mut u8, usize, &mut Option<u64>)> {
        self.class_mut(class).regions.iter_mut().map(|r| (r.ptr, REGION_SIZE, &mut r.rio_buffer_id))
    }

    pub fn any_large_pages(&self) -> bool {
        self.classes.iter().any(|c| c.regions.iter().any(|r| r.large_pages))
    }

    /// True once after any slab growth (R-043 registration cue).
    pub fn take_regions_dirty(&mut self) -> bool {
        std::mem::take(&mut self.regions_dirty)
    }

    /// All not-yet-registered regions across every class, for one
    /// `register_buffers` call (the backend fills the cookies).
    pub fn unregistered_regions_mut(&mut self) -> Vec<(*mut u8, usize, &mut Option<u64>)> {
        self.classes
            .iter_mut()
            .flat_map(|c| c.regions.iter_mut())
            .filter(|r| r.rio_buffer_id.is_none())
            .map(|r| (r.ptr, REGION_SIZE, &mut r.rio_buffer_id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_stale_slot_id_cannot_free_someone_elses_buffer() {
        // The dangerous shape: A releases, the slot is recycled to B, and
        // A releases again (a double-release, or an id kept past its
        // handover). Without a generation the index is still in range and
        // the refcount is still positive, so the second release frees a
        // buffer B is actively using -- two owners, same memory.
        let mut p = BufferPool::new();
        let a = p.acquire(SizeClass::S64K);
        assert!(p.release(a));
        let b = p.acquire(SizeClass::S64K);
        assert_eq!(a.index, b.index, "test needs the slot to be recycled");
        assert_ne!(a.generation, b.generation);

        assert!(!p.release(a), "stale id freed the slot its successor owns");
        assert_eq!(p.stale_rejections(), 1);
        assert_eq!(p.refcount(b), 1, "B's buffer was released out from under it");
        assert_eq!(p.refcount(a), 0, "a stale id must not report a live count");

        // B's own lifecycle still works.
        p.retain(b);
        assert_eq!(p.refcount(b), 2);
        assert!(!p.release(b));
        assert!(p.release(b));
    }

    #[test]
    fn an_out_of_range_slot_id_is_not_live() {
        let mut p = BufferPool::new();
        let bogus = SlotId { class: SizeClass::S64K, index: 9_999_999, generation: 0 };
        assert_eq!(p.refcount(bogus), 0);
        assert!(!p.release(bogus));
        p.retain(bogus); // must not panic
        assert_eq!(p.stale_rejections(), 2);
    }

    use super::*;
    use proptest::prelude::*;

    #[test]
    fn acquire_release_recycles() {
        let mut p = BufferPool::new();
        let a = p.acquire(SizeClass::S4K);
        assert_eq!(p.in_use(), 1);
        assert!(p.release(a));
        assert_eq!(p.in_use(), 0);
        let b = p.acquire(SizeClass::S4K);
        assert_eq!(b.index, a.index, "LIFO freelist should recycle the slot");
        p.release(b);
    }

    #[test]
    fn slot_survives_while_referenced() {
        let mut p = BufferPool::new();
        let a = p.acquire(SizeClass::S16K);
        p.retain(a); // e.g. exported memoryview
        assert!(!p.release(a), "kernel-op ref dropped, view still alive");
        assert_eq!(p.in_use(), 1);
        assert!(p.release(a), "last ref frees");
        assert_eq!(p.in_use(), 0);
    }

    #[test]
    fn poison_on_free_in_debug() {
        if !cfg!(debug_assertions) {
            return;
        }
        let mut p = BufferPool::new();
        let a = p.acquire(SizeClass::S4K);
        let ptr = p.slot_ptr(a);
        unsafe { std::ptr::write_bytes(ptr, 0xAB, 64) };
        p.release(a);
        // Slot is free; peek memory through a fresh acquire of same slot.
        let b = p.acquire(SizeClass::S4K);
        assert_eq!(b.index, a.index);
        let bytes = unsafe { std::slice::from_raw_parts(p.slot_ptr(b), 64) };
        assert!(bytes.iter().all(|&x| x == 0xDD), "freed slot not poisoned");
        p.release(b);
    }

    #[test]
    fn distinct_slots_do_not_overlap() {
        let mut p = BufferPool::new();
        let ids: Vec<_> = (0..10).map(|_| p.acquire(SizeClass::S4K)).collect();
        let mut ranges: Vec<_> = ids
            .iter()
            .map(|&id| (p.slot_ptr(id) as usize, p.slot_ptr(id) as usize + id.class.size()))
            .collect();
        ranges.sort();
        for w in ranges.windows(2) {
            assert!(w[0].1 <= w[1].0, "slots overlap");
        }
        for id in ids {
            p.release(id);
        }
    }

    #[test]
    fn growth_past_one_region() {
        let mut p = BufferPool::new();
        let n = SizeClass::S64K.slots_per_region() + 3;
        let ids: Vec<_> = (0..n).map(|_| p.acquire(SizeClass::S64K)).collect();
        assert_eq!(p.in_use(), n);
        for id in ids {
            p.release(id);
        }
        assert_eq!(p.in_use(), 0);
    }

    #[test]
    fn fitting_class_selection() {
        assert_eq!(SizeClass::fitting(1), Some(SizeClass::S4K));
        assert_eq!(SizeClass::fitting(4096), Some(SizeClass::S4K));
        assert_eq!(SizeClass::fitting(4097), Some(SizeClass::S16K));
        assert_eq!(SizeClass::fitting(65536), Some(SizeClass::S64K));
        assert_eq!(SizeClass::fitting(65537), None);
    }

    /// R-121: property test for buffer slot lifecycle. Random
    /// acquire/retain/release sequences never corrupt refcounts, never free
    /// a referenced slot, and `in_use` is always exact.
    #[derive(Debug, Clone)]
    enum Op {
        Acquire(u8),
        Retain(usize),
        Release(usize),
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (0u8..3).prop_map(Op::Acquire),
            2 => (0usize..64).prop_map(Op::Retain),
            4 => (0usize..64).prop_map(Op::Release),
        ]
    }

    proptest! {
        #[test]
        fn slot_lifecycle_holds(ops in proptest::collection::vec(op_strategy(), 0..200)) {
            let mut p = BufferPool::new();
            let mut live: Vec<(SlotId, u32)> = Vec::new();
            for op in ops {
                match op {
                    Op::Acquire(c) => {
                        let class = [SizeClass::S4K, SizeClass::S16K, SizeClass::S64K][c as usize];
                        let id = p.acquire(class);
                        prop_assert_eq!(p.refcount(id), 1);
                        live.push((id, 1));
                    }
                    Op::Retain(i) => {
                        if !live.is_empty() {
                            let idx = i % live.len();
                            let entry = &mut live[idx];
                            p.retain(entry.0);
                            entry.1 += 1;
                            prop_assert_eq!(p.refcount(entry.0), entry.1);
                        }
                    }
                    Op::Release(i) => {
                        if !live.is_empty() {
                            let idx = i % live.len();
                            let freed = p.release(live[idx].0);
                            live[idx].1 -= 1;
                            prop_assert_eq!(freed, live[idx].1 == 0);
                            if live[idx].1 == 0 {
                                live.remove(idx);
                            }
                        }
                    }
                }
                prop_assert_eq!(p.in_use(), live.len());
            }
        }
    }
}
