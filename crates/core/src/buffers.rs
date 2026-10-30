//! Buffer slabs (R-071, R-072, R-073).
//!
//! Size classes {4 KiB, 16 KiB, 64 KiB}. Each slab is one 2 MiB region
//! (`VirtualAlloc` on Windows -- reserved, and committed slot by slot as
//! slots are handed out, unless large pages (`MEM_LARGE_PAGES`, when
//! `SeLockMemoryPrivilege` is held) or buffer registration need it
//! committed whole; an anonymous `mmap` on Linux; `alloc_zeroed` elsewhere
//! for dev/test builds), carved into fixed-size slots with a per-class
//! freelist.
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

    /// The class for a connection's next receive buffer, after a read
    /// returned `bytes` into one of this class. A full buffer grows it a
    /// class (more may be waiting). Only a read far below the current size
    /// shrinks it, and never one that `follows_full` -- the previous read
    /// filled its buffer, so this one is the tail of a larger message: a
    /// 16.5 KB request filled 16 KiB, grew the buffer, then shrank it to
    /// 4 KiB on its 100-byte tail, and every request took three reads.
    /// An idle connection still settles on the smallest.
    pub fn after_read(self, bytes: usize, follows_full: bool) -> SizeClass {
        if bytes >= self.size() {
            return match self {
                SizeClass::S4K => SizeClass::S16K,
                _ => SizeClass::S64K,
            };
        }
        if follows_full {
            return self;
        }
        if bytes <= 1024 {
            return SizeClass::S4K;
        }
        if self == SizeClass::S64K && bytes <= 4096 {
            return SizeClass::S16K;
        }
        self
    }
}

#[cfg(test)]
mod after_read_tests {
    use super::SizeClass::{self, S16K, S4K, S64K};

    #[test]
    fn full_reads_grow_one_class_at_a_time() {
        assert_eq!(S4K.after_read(4096, false), S16K);
        assert_eq!(S16K.after_read(16384, true), S64K);
        assert_eq!(S64K.after_read(65536, true), S64K);
    }

    #[test]
    fn small_reads_settle_on_the_smallest_and_tails_do_not_flap() {
        assert_eq!(S64K.after_read(300, false), S4K, "an idle keep-alive request");
        assert_eq!(S64K.after_read(3000, false), S16K);
        assert_eq!(S64K.after_read(20_000, false), S64K, "a bulk stream's short read keeps 64K");
        assert_eq!(S16K.after_read(5000, false), S16K);
        assert_eq!(S4K.after_read(2000, false), S4K);
        let mut c = SizeClass::S4K;
        for _ in 0..3 {
            c = c.after_read(c.size(), true);
        }
        assert_eq!(c, S64K, "a bulk transfer reaches 64 KiB reads after two full ones");
    }

    #[test]
    fn a_messages_tail_does_not_shrink_the_buffer_it_grew() {
        // A 16.5 KB request per round trip: one read per request once
        // settled, not a grow/shrink cycle of three.
        let (mut c, mut full, mut reads) = (S4K, false, 0);
        for _ in 0..10 {
            let mut left = 16_500usize;
            while left > 0 {
                let got = left.min(c.size());
                left -= got;
                reads += 1;
                let filled = got >= c.size();
                c = c.after_read(got, full);
                full = filled;
            }
        }
        assert_eq!(c, S64K);
        assert!(reads <= 10 + 3, "{reads} reads for 10 requests");
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
    /// Do its slots count against the OS before they are first used? Yes
    /// for an allocation committed whole (Windows large pages, or a region
    /// committed for registration) and for `alloc_zeroed`. No for a Linux
    /// mapping, faulted in page by page, or a Windows reservation, whose
    /// slots `page_in` commits as `acquire` hands them out.
    fresh_held: bool,
}

impl Region {
    /// May its free slots be paged out? Not once RIO has registered it
    /// (the kernel holds its pages), nor large pages (not decommittable
    /// piecewise).
    fn pageable(&self) -> bool {
        self.rio_buffer_id.is_none() && !self.large_pages
    }
}

// Thread-affine by contract (loop thread only); pointers make this !Send by
// default which is exactly right. No unsafe Send/Sync impls.

impl Region {
    /// `commit`: commit the region whole now (Windows), for a backend that
    /// registers regions with the kernel -- RIO locks a region's pages when
    /// it registers it, so they must exist.
    fn alloc(commit: bool) -> Region {
        #[cfg(windows)]
        {
            use windows_sys::Win32::System::Memory::{
                VirtualAlloc, MEM_COMMIT, MEM_LARGE_PAGES, MEM_RESERVE, PAGE_READWRITE,
            };
            // R-071: attempt large pages, silent fallback. They can only be
            // allocated committed (and locked), whole.
            let ptr = unsafe {
                VirtualAlloc(
                    std::ptr::null(),
                    REGION_SIZE,
                    MEM_COMMIT | MEM_RESERVE | MEM_LARGE_PAGES,
                    PAGE_READWRITE,
                )
            };
            if !ptr.is_null() {
                return Region { ptr: ptr.cast(), rio_buffer_id: None, large_pages: true, fresh_held: true };
            }
            // Otherwise reserved only: `page_in` commits each slot as it is
            // first handed out. Committed whole, a region charged its full
            // 2 MiB of commit for slots no connection had ever used -- and
            // `bytes_held`, which counts a never-used slot as holding
            // nothing, reported a fraction of it (one region with 92 of its
            // 512 4 KiB slots ever used: 368 KiB reported, 2 MiB committed).
            let flags = if commit { MEM_COMMIT | MEM_RESERVE } else { MEM_RESERVE };
            let ptr = unsafe { VirtualAlloc(std::ptr::null(), REGION_SIZE, flags, PAGE_READWRITE) };
            assert!(!ptr.is_null(), "VirtualAlloc({REGION_SIZE}) failed");
            Region { ptr: ptr.cast(), rio_buffer_id: None, large_pages: false, fresh_held: commit }
        }
        #[cfg(target_os = "linux")]
        {
            // Nothing registers buffers on Linux; there is nothing to commit.
            let _ = commit;
            // Straight from the kernel, not the global allocator. Under
            // mimalloc (the extension's allocator, R-070) `alloc_zeroed` of
            // a 2 MiB region came back fully resident, so every connection's
            // 64 KiB receive slot cost 64 KiB of RSS even when no request
            // ever filled more than a page of it: 3000 idle keep-alive
            // connections held ~190 MB. An anonymous mapping is zero-filled
            // and faulted in page by page as recvs actually write.
            //
            // Transparent huge pages stay off for these regions: with THP
            // "always", a recent kernel aligns a 2 MiB anonymous mapping to
            // a huge-page boundary and the first byte received would fault
            // in the whole region again.
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    REGION_SIZE,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                )
            };
            assert!(ptr != libc::MAP_FAILED, "mmap({REGION_SIZE}) failed");
            // Advisory: a kernel without THP refuses it, and then there are
            // no huge pages to avoid.
            unsafe { libc::madvise(ptr, REGION_SIZE, libc::MADV_NOHUGEPAGE) };
            Region { ptr: ptr.cast(), rio_buffer_id: None, large_pages: false, fresh_held: false }
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        {
            let _ = commit;
            let layout = std::alloc::Layout::from_size_align(REGION_SIZE, 4096).unwrap();
            let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!ptr.is_null(), "region alloc failed");
            Region { ptr, rio_buffer_id: None, large_pages: false, fresh_held: true }
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
        #[cfg(target_os = "linux")]
        unsafe {
            libc::munmap(self.ptr.cast(), REGION_SIZE);
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        unsafe {
            let layout = std::alloc::Layout::from_size_align(REGION_SIZE, 4096).unwrap();
            std::alloc::dealloc(self.ptr, layout);
        }
    }
}

/// Free slots per class that keep their pages when released. Past this,
/// a released slot's pages go back to the OS (Linux; see `page_out`).
///
/// Regions are never freed, so without a bound the pool held its peak:
/// after 3000 connections that each received 200 KB had all closed, the
/// 190 MB their receive slots had touched stayed resident for good. The
/// most recently freed slots are handed out first, so a steady
/// connection churn below this many free slots never pages anything out
/// or faults anything back in; 256 64 KiB slots is 16 MiB.
pub const RESIDENT_FREE_SLOTS: usize = 256;

/// Whether `page_out` actually returns memory on this platform.
///
/// On Windows a slot, once committed, is private memory (commit charge)
/// whether or not it is in use, and a pool that never decommitted held its
/// peak for good: 5,000 idle connections closing left 256 MiB committed.
/// Regions RIO has registered with the kernel, and large-page regions
/// (which cannot be decommitted piecewise), keep every page
/// (`Region::pageable`).
const PAGES_OUT: bool = cfg!(any(target_os = "linux", windows));

/// Hand a free slot's pages back to the OS. On Linux they read as zero
/// when next touched; on Windows they are decommitted and `page_in` must
/// recommit them before the slot is used again. Returns whether it did.
fn page_out(ptr: *mut u8, len: usize) -> bool {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: a whole free slot inside one of our private anonymous
        // mappings; nothing references it (refcount zero) until it is
        // handed out again.
        unsafe { libc::madvise(ptr.cast(), len, libc::MADV_DONTNEED) == 0 }
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Memory::{VirtualFree, MEM_DECOMMIT};
        // SAFETY: a whole free slot (page-aligned: slots are 4 KiB
        // multiples in a 64 KiB-aligned region) that nothing references
        // until `acquire` hands it out again, recommitting it first.
        unsafe { VirtualFree(ptr.cast(), len, MEM_DECOMMIT) != 0 }
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let _ = (ptr, len);
        false
    }
}

/// Make a paged-out slot usable again (Windows: recommit it; elsewhere its
/// pages fault back in on their own).
fn page_in(ptr: *mut u8, len: usize) {
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Memory::{VirtualAlloc, MEM_COMMIT, PAGE_READWRITE};
        let p = unsafe { VirtualAlloc(ptr.cast(), len, MEM_COMMIT, PAGE_READWRITE) };
        // The same failure Region::alloc treats as fatal: no commit left.
        assert!(!p.is_null(), "VirtualAlloc(MEM_COMMIT, {len}) of a buffer slot failed");
    }
    #[cfg(not(windows))]
    {
        let _ = (ptr, len);
    }
}

struct ClassPool {
    class: SizeClass,
    regions: Vec<Region>,
    /// Free slots. Resident ones are pushed at the back and handed out
    /// first; paged-out ones (`paged_out`) wait at the front.
    freelist: std::collections::VecDeque<u32>,
    /// Refcount per allocated slot index; 0 == free.
    refcounts: Vec<u32>,
    /// Parallel to `refcounts`; see `SlotId::generation`.
    generations: Vec<u32>,
    /// Parallel to `refcounts`: a free slot with no resident pages (never
    /// touched since its region was mapped, or paged out on release).
    paged_out: Vec<bool>,
    /// Free slots that are not `paged_out`, against RESIDENT_FREE_SLOTS.
    resident_free: usize,
    /// `retain`/`release` calls refused because the id was stale or out of
    /// range. Always zero in a correct run.
    stale_rejections: u64,
    in_use: usize,
    /// Commit new regions whole (`BufferPool::set_commit_regions`).
    commit_regions: bool,
}

impl ClassPool {
    fn new(class: SizeClass) -> Self {
        ClassPool {
            class,
            regions: Vec::new(),
            freelist: std::collections::VecDeque::new(),
            refcounts: Vec::new(),
            generations: Vec::new(),
            paged_out: Vec::new(),
            resident_free: 0,
            stale_rejections: 0,
            in_use: 0,
            commit_regions: false,
        }
    }

    fn grow(&mut self) {
        self.add_region(Region::alloc(self.commit_regions));
    }

    fn add_region(&mut self, region: Region) {
        let base = (self.regions.len() * self.class.slots_per_region()) as u32;
        let held = region.fresh_held;
        self.regions.push(region);
        let n = self.class.slots_per_region() as u32;
        self.refcounts.resize(self.refcounts.len() + n as usize, 0);
        self.generations.resize(self.generations.len() + n as usize, 0);
        // A fresh slot holds nothing from the OS until it is first used --
        // unless its region came committed (`Region::fresh_held`), in which
        // case it is resident (no `page_in` on acquire) and counted so.
        self.paged_out.resize(self.paged_out.len() + n as usize, !held);
        if held {
            self.resident_free += n as usize;
        }
        // Only ever grown empty; pushed in reverse so low indices are
        // handed out first.
        for i in (0..n).rev() {
            self.freelist.push_back(base + i);
        }
    }

    /// Is this id both in range and current? Bounds are checked because an
    /// id can outlive the pool state it names, and an out-of-range index
    /// should resolve to "not live" rather than panic.
    fn live(&self, id: SlotId) -> bool {
        let i = id.index as usize;
        self.refcounts.get(i).is_some_and(|&rc| rc > 0) && self.generations.get(i) == Some(&id.generation)
    }

    fn pageable(&self, index: u32) -> bool {
        self.regions[index as usize / self.class.slots_per_region()].pageable()
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
        let index = pool.freelist.pop_back().expect("freelist refilled by grow");
        debug_assert_eq!(pool.refcounts[index as usize], 0, "acquired slot has live refs");
        if std::mem::take(&mut pool.paged_out[index as usize]) {
            page_in(pool.slot_ptr(index), class.size());
        } else {
            pool.resident_free -= 1;
        }
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
        if PAGES_OUT
            && pool.resident_free >= RESIDENT_FREE_SLOTS
            && pool.pageable(id.index)
            && page_out(pool.slot_ptr(id.index), size)
        {
            pool.paged_out[id.index as usize] = true;
            pool.freelist.push_front(id.index);
        } else {
            pool.resident_free += 1;
            pool.freelist.push_back(id.index);
        }
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

    /// Bytes of the buffers currently held.
    pub fn bytes_in_use(&self) -> usize {
        self.classes.iter().map(|c| c.in_use * c.class.size()).sum()
    }

    /// Bytes the pool keeps from the OS: its regions, less the free slots
    /// that hold nothing -- never used, or paged out (see `PAGES_OUT`). On
    /// Windows, its commit charge; on Linux, an upper bound on its resident
    /// pages (a used slot's untouched pages are counted).
    pub fn bytes_held(&self) -> usize {
        self.classes
            .iter()
            .map(|c| {
                (c.regions.len() * REGION_SIZE)
                    .saturating_sub((c.freelist.len() - c.resident_free) * c.class.size())
            })
            .sum()
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

    /// Commit each region whole as it is allocated (Windows), for a
    /// backend that registers regions with the kernel: RIO locks a
    /// region's pages when it registers it, so they must already be
    /// committed. Set before the first `acquire`; elsewhere a region is
    /// committed slot by slot, as slots are first handed out.
    pub fn set_commit_regions(&mut self, commit: bool) {
        for c in &mut self.classes {
            c.commit_regions = commit;
        }
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

    /// Every free slot is either counted resident or marked paged out.
    fn assert_free_accounting(p: &BufferPool, class: SizeClass) {
        let c = p.class(class);
        let paged_out = c.freelist.iter().filter(|&&i| c.paged_out[i as usize]).count();
        assert_eq!(c.resident_free + paged_out, c.freelist.len());
        assert!(c.freelist.iter().all(|&i| c.refcounts[i as usize] == 0));
    }

    /// Windows: slots released past the warm set are decommitted -- the
    /// pool's commit charge falls -- and a decommitted slot handed out
    /// again is recommitted before use (a write to it would fault if not).
    #[cfg(windows)]
    #[test]
    fn released_slots_are_decommitted_and_recommitted_on_reuse() {
        use windows_sys::Win32::System::Memory::{VirtualQuery, MEMORY_BASIC_INFORMATION, MEM_COMMIT};
        let state = |ptr: *mut u8| {
            let mut m: MEMORY_BASIC_INFORMATION = unsafe { std::mem::zeroed() };
            unsafe { VirtualQuery(ptr.cast(), &mut m, std::mem::size_of::<MEMORY_BASIC_INFORMATION>()) };
            m.State
        };
        let mut p = BufferPool::new();
        let n = RESIDENT_FREE_SLOTS * 2;
        let ids: Vec<SlotId> = (0..n).map(|_| p.acquire(SizeClass::S16K)).collect();
        let held = p.bytes_held();
        let ptrs: Vec<*mut u8> = ids.iter().map(|&id| p.slot_ptr(id)).collect();
        for &id in &ids {
            p.release(id);
        }
        assert_eq!(held - p.bytes_held(), RESIDENT_FREE_SLOTS * SizeClass::S16K.size(), "past the warm set");
        let decommitted = ptrs.iter().filter(|&&q| state(q) != MEM_COMMIT).count();
        assert_eq!(decommitted, RESIDENT_FREE_SLOTS);
        let again: Vec<SlotId> = (0..n).map(|_| p.acquire(SizeClass::S16K)).collect();
        for &id in &again {
            unsafe { std::ptr::write_bytes(p.slot_ptr(id), 0xAB, SizeClass::S16K.size()) };
        }
        assert_eq!(p.bytes_held(), held);
        for id in again {
            p.release(id);
        }
    }

    /// Windows: a region is reserved and its slots committed one by one as
    /// they are handed out, so a slot never used is neither committed nor
    /// counted -- unless the backend registers regions (RIO), which needs
    /// them committed whole, and then all of it is counted.
    #[cfg(windows)]
    #[test]
    fn a_region_is_committed_slot_by_slot_unless_registration_needs_it_whole() {
        use windows_sys::Win32::System::Memory::{
            VirtualQuery, MEMORY_BASIC_INFORMATION, MEM_COMMIT, MEM_RESERVE,
        };
        let state = |ptr: *mut u8| {
            let mut m: MEMORY_BASIC_INFORMATION = unsafe { std::mem::zeroed() };
            unsafe { VirtualQuery(ptr.cast(), &mut m, std::mem::size_of::<MEMORY_BASIC_INFORMATION>()) };
            m.State
        };
        let size = SizeClass::S16K.size();
        let mut p = BufferPool::new();
        let a = p.acquire(SizeClass::S16K);
        if !p.any_large_pages() {
            let first = p.slot_ptr(a);
            assert_eq!(state(first), MEM_COMMIT);
            assert_eq!(state(unsafe { first.add(size) }), MEM_RESERVE, "a slot never handed out");
            assert_eq!(p.bytes_held(), size);
        }
        p.release(a);
        let mut q = BufferPool::new();
        q.set_commit_regions(true);
        let b = q.acquire(SizeClass::S16K);
        assert_eq!(state(unsafe { q.slot_ptr(b).add(size) }), MEM_COMMIT, "committed whole");
        assert_eq!(q.bytes_held(), REGION_SIZE);
        q.release(b);
    }

    /// `bytes_held` counts a slot never used as holding nothing only where
    /// that is true: a lazily faulted Linux mapping, or a Windows region
    /// reserved and committed slot by slot. One that came committed whole
    /// counted the same way under-reported its commit charge (368 KiB
    /// reported of 2 MiB committed); its slots are now held, resident and
    /// never paged in.
    #[test]
    fn never_used_slots_count_as_held_only_in_a_region_that_came_committed() {
        let size = SizeClass::S4K.size();
        let mut p = BufferPool::new();
        let ids: Vec<SlotId> = (0..3).map(|_| p.acquire(SizeClass::S4K)).collect();
        let expected = if cfg!(any(windows, target_os = "linux")) { 3 * size } else { REGION_SIZE };
        if !p.any_large_pages() {
            assert_eq!(p.bytes_held(), expected);
        }
        assert_free_accounting(&p, SizeClass::S4K);
        for id in ids {
            assert!(p.release(id));
        }

        // A region that came committed whole: large pages, a region RIO
        // registers, or the portable fallback.
        let mut q = BufferPool::new();
        let mut region = Region::alloc(false);
        region.fresh_held = true;
        q.class_mut(SizeClass::S4K).add_region(region);
        assert_eq!(q.bytes_held(), REGION_SIZE);
        assert_free_accounting(&q, SizeClass::S4K);
        assert_eq!(q.class(SizeClass::S4K).resident_free, SizeClass::S4K.slots_per_region());
        let id = q.acquire(SizeClass::S4K);
        assert_eq!(q.bytes_held(), REGION_SIZE, "using a committed slot commits nothing more");
        assert!(!q.class(SizeClass::S4K).paged_out[id.index as usize]);
        assert_free_accounting(&q, SizeClass::S4K);
        // Past the warm set, as it is here, a released slot of a pageable
        // region is paged out.
        assert!(q.release(id));
        let paged = if PAGES_OUT { size } else { 0 };
        assert_eq!(q.bytes_held(), REGION_SIZE - paged);
        assert_free_accounting(&q, SizeClass::S4K);
    }

    #[test]
    fn release_past_the_resident_budget_pages_out_and_keeps_the_warm_set_first() {
        let mut p = BufferPool::new();
        let n = RESIDENT_FREE_SLOTS * 2 + 7;
        let ids: Vec<SlotId> = (0..n).map(|_| p.acquire(SizeClass::S64K)).collect();
        for &id in &ids {
            assert!(p.release(id));
        }
        assert_free_accounting(&p, SizeClass::S64K);
        let resident_expected = if PAGES_OUT { RESIDENT_FREE_SLOTS } else { n };
        assert_eq!(p.class(SizeClass::S64K).resident_free, resident_expected);
        // Reacquired, the resident slots come first: the last ones freed
        // within the budget, most recent first.
        let warm = p.acquire(SizeClass::S64K);
        assert_eq!(warm.index, ids[RESIDENT_FREE_SLOTS - 1].index);
        assert_free_accounting(&p, SizeClass::S64K);
        // Every slot is still usable, and ids still go stale on recycle.
        let again: Vec<SlotId> = (0..n - 1).map(|_| p.acquire(SizeClass::S64K)).collect();
        assert_free_accounting(&p, SizeClass::S64K);
        assert_eq!(p.in_use(), n);
        assert!(!p.release(ids[0]), "a stale id still resolves");
        for id in again.into_iter().chain([warm]) {
            assert!(p.release(id));
        }
        assert_free_accounting(&p, SizeClass::S64K);
        assert_eq!(p.in_use(), 0);
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
