//! Buffer slabs cost memory for what is written to them, not for what is
//! reserved.
//!
//! A slab region is 2 MiB and a receive slot 64 KiB, held by its
//! connection for life. Allocated with `alloc_zeroed` through mimalloc --
//! the extension's global allocator -- every region came back fully
//! resident, so each idle connection cost its whole 64 KiB slot in RSS.
//! This binary installs the same allocator, so it checks the configuration
//! that ships rather than whatever the test harness happens to use.
#![cfg(target_os = "linux")]

use cadeloop_core::buffers::{BufferPool, SizeClass, REGION_SIZE, RESIDENT_FREE_SLOTS};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Pages of `[ptr, ptr + len)` that are resident right now (mincore(2)).
fn resident_pages(ptr: *const u8, len: usize) -> usize {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    let start = ptr as usize & !(page - 1);
    let end = (ptr as usize + len).div_ceil(page) * page;
    let mut vec = vec![0u8; (end - start) / page];
    let rc = unsafe { libc::mincore(start as *mut libc::c_void, end - start, vec.as_mut_ptr()) };
    assert_eq!(rc, 0, "mincore: {}", std::io::Error::last_os_error());
    vec.iter().filter(|&&b| b & 1 != 0).count()
}

#[test]
fn acquired_slots_are_not_resident_until_written() {
    let mut pool = BufferPool::new();
    let per_region = REGION_SIZE / SizeClass::S64K.size();
    // Three regions' worth: the first slot of each region is what a
    // fresh connection would be handed.
    let ids: Vec<_> = (0..per_region * 3).map(|_| pool.acquire(SizeClass::S64K)).collect();
    let resident: usize = ids.iter().map(|&id| resident_pages(pool.slot_ptr(id), 65536)).sum();
    assert_eq!(resident, 0, "freshly acquired 64 KiB slots are already resident ({resident} pages)");

    // A small read touches one page of its slot, and only that page.
    let slot = ids[per_region];
    unsafe { pool.slot_ptr(slot).write(1) };
    assert_eq!(resident_pages(pool.slot_ptr(slot), 65536), 1);
    let rest: usize =
        ids.iter().filter(|&&id| id != slot).map(|&id| resident_pages(pool.slot_ptr(id), 65536)).sum();
    assert_eq!(rest, 0, "writing one slot faulted in its neighbours");

    for id in ids {
        pool.release(id);
    }
}

/// Resident pages across every 64 KiB-class region of `pool`.
fn pool_resident_pages(pool: &mut BufferPool) -> usize {
    pool.regions_mut(SizeClass::S64K).map(|(ptr, len, _)| resident_pages(ptr, len)).sum()
}

#[test]
fn a_burst_gives_its_pages_back_once_it_is_over() {
    // A connection burst: every receive slot filled, then every
    // connection gone. Only the warm set of free slots may stay resident.
    let mut pool = BufferPool::new();
    let n = RESIDENT_FREE_SLOTS * 4;
    let ids: Vec<_> = (0..n).map(|_| pool.acquire(SizeClass::S64K)).collect();
    for &id in &ids {
        unsafe { std::ptr::write_bytes(pool.slot_ptr(id), 0x5A, 65536) };
    }
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    let per_slot = 65536 / page;
    assert_eq!(pool_resident_pages(&mut pool), n * per_slot);
    for id in ids {
        pool.release(id);
    }
    let kept = pool_resident_pages(&mut pool);
    assert!(
        kept <= RESIDENT_FREE_SLOTS * per_slot,
        "{kept} pages still resident after the burst; the budget is {}",
        RESIDENT_FREE_SLOTS * per_slot
    );

    // Churn below the budget pages nothing out: a reused slot keeps the
    // pages it already had, so steady traffic never faults them back in.
    let before = pool_resident_pages(&mut pool);
    for _ in 0..1000 {
        let id = pool.acquire(SizeClass::S64K);
        unsafe { pool.slot_ptr(id).write(1) };
        pool.release(id);
    }
    assert_eq!(pool_resident_pages(&mut pool), before);

    // Paged-out slots are handed out again, zeroed and fully usable.
    let ids: Vec<_> = (0..n).map(|_| pool.acquire(SizeClass::S64K)).collect();
    for &id in &ids {
        let p = pool.slot_ptr(id);
        unsafe { std::ptr::write_bytes(p, 0xC3, 65536) };
        assert!(unsafe { std::slice::from_raw_parts(p, 65536) }.iter().all(|&b| b == 0xC3));
    }
    for id in ids {
        pool.release(id);
    }
}

#[test]
fn slots_start_zeroed_and_keep_what_is_written() {
    // The region must still behave like alloc_zeroed's: zero-filled on
    // first use, and stable once written.
    let mut pool = BufferPool::new();
    let a = pool.acquire(SizeClass::S16K);
    let b = pool.acquire(SizeClass::S16K);
    let pa = pool.slot_ptr(a);
    assert!(unsafe { std::slice::from_raw_parts(pa, 16384) }.iter().all(|&x| x == 0));
    unsafe { std::ptr::write_bytes(pa, 0xAB, 16384) };
    assert!(unsafe { std::slice::from_raw_parts(pool.slot_ptr(b), 16384) }.iter().all(|&x| x == 0));
    assert!(unsafe { std::slice::from_raw_parts(pa, 16384) }.iter().all(|&x| x == 0xAB));
    pool.release(a);
    pool.release(b);
}
