//! Registered I/O backend (R-040..R-044, milestone M3) — a hybrid over
//! the IOCP backend.
//!
//! Division of labor:
//! * IOCP (inner): accepts (AcceptEx), connects (ConnectEx), disconnect
//!   recycling, readiness probes (R-057), and the cross-thread wakeup —
//!   RIO has no equivalents for any of these.
//! * RIO: recv/send on connected sockets that carry a request queue.
//!   Accept sockets are created with `WSA_FLAG_REGISTERED_IO` (see the
//!   inner backend's `accept_socket_flags`); sockets whose RQ creation is
//!   refused (foreign fds without the flag, e.g. from `create_connection`
//!   via netsys) transparently fall back to the inner IOCP ops.
//!
//! Completion delivery (R-041): ONE completion queue per loop, sized
//! `cfg.rio_cq_size` and grown by doubling (RIOResizeCompletionQueue) as
//! request queues reserve capacity — RIO reserves CQ slots at RQ-creation
//! time, so "CQ overflow" is a creation-time refusal here, never silent
//! loss (§16). Notification integrates with the inner backend's IOCP port
//! (`RIO_IOCP_COMPLETION`, completion key `KEY_RIO`): the poll drains
//! `RIODequeueCompletion` directly first (spin path, R-060) and arms
//! `RIONotify` only before actually parking.
//!
//! Buffers (R-043): recv buffers come from the loop's slab pool, whose
//! 2 MiB regions are registered once via the `register_buffers` trait
//! hook (pointer → (RIO_BUFFERID, offset) resolution in
//! `rio_util::RegionMap`). Sends are copied into 64 KiB staging slots
//! carved from backend-owned registered regions (RIO takes exactly one
//! registered buffer per request, so arbitrary Python-owned gather
//! payloads cannot be posted zero-copy); a payload larger than a slot is
//! sent in slot-sized parts via the transport layer's partial-send
//! resumption. Staging capacity grows a region at a time.
//!
//! Cancellation: RIO has no CancelIoEx. `cancel` marks the op; the
//! completion still arrives (on data, error, or `closesocket`, which
//! flushes outstanding requests through the CQ) and is translated with
//! `WSA_OPERATION_ABORTED` so upper layers observe IOCP-identical
//! semantics (R-037).
//!
//! Status: compile-verified (msvc cross-check + Windows CI build); the
//! platform-independent bookkeeping is unit-tested in `rio_util`.
//! Behavioral validation on Windows hardware is the remaining M3 gate,
//! so `backend="auto"` keeps resolving to IOCP until then.
//!
//! Hardware finding (2026-08, examples/rio_probe.rs): on Windows 11
//! beta build 26200.9168 (native x64), the RIO subsystem itself fails
//! to initialize — RIORegisterBuffer AND RIOCreateCompletionQueue (all
//! notification variants, all sizes, socket alive/bound/listening or
//! not) return their failure sentinels with a freshly-set WSAEFAULT,
//! despite the function table resolving from genuine unhooked
//! mswsock.dll, a pristine LSP-free Winsock catalog, and argument
//! lists containing no pointer (null-notification CQ). The failure is
//! in mswsock's private first-use handshake with afd.sys, below
//! anything reachable from user code. Construction failure is
//! surfaced with both named errors; callers (validate.ps1's gate,
//! serve()) treat it as RIO-unavailable and stay on IOCP. Behavioral
//! validation needs a stable x64 build (23H2/24H2 or Server).

use std::collections::HashMap;
use std::io;
use std::mem::{size_of, zeroed};
use std::sync::Arc;
use std::time::Duration;

use windows_sys::core::GUID;
use windows_sys::Win32::Foundation::{GetLastError, HANDLE, WAIT_TIMEOUT};
use windows_sys::Win32::Networking::WinSock::{
    closesocket, WSAGetLastError, WSAIoctl, WSASocketW, AF_INET, IPPROTO_TCP,
    RIORESULT, RIO_BUF, RIO_BUFFERID, RIO_CORRUPT_CQ, RIO_CQ,
    RIO_EXTENSION_FUNCTION_TABLE, RIO_IOCP_COMPLETION, RIO_NOTIFICATION_COMPLETION,
    RIO_NOTIFICATION_COMPLETION_0, RIO_NOTIFICATION_COMPLETION_0_1, RIO_RQ,
    SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER, SOCKET, SOCKET_ERROR,
    WSAID_MULTIPLE_RIO, WSA_FLAG_OVERLAPPED, WSA_FLAG_REGISTERED_IO,
    WSA_OPERATION_ABORTED,
};
use windows_sys::Win32::System::Memory::{
    VirtualAlloc, VirtualFree, MEM_COMMIT, MEM_RELEASE, MEM_RESERVE, PAGE_READWRITE,
};
use windows_sys::Win32::System::IO::{GetQueuedCompletionStatusEx, OVERLAPPED, OVERLAPPED_ENTRY};

use super::iocp::{IocpBackend, KEY_RIO, POLL_BATCH};
use super::rio_util::{CqLedger, RegionMap, StagingLedger};
use super::{Completion, IoBackend, IoSlice, RawSocket, Wakeup};
use crate::opslab::{OpId, OpKind, OpSlab};

/// Sentinels per MSWSock headers (not exported by windows-sys).
/// RIO_INVALID_BUFFERID is `((RIO_BUFFERID)0xFFFFFFFF)` in the header: a
/// 32-bit unsigned constant, so it ZERO-extends to 0x00000000FFFFFFFF on
/// x64 — comparing against sign-extended -1 silently misses real
/// failures (run-6 probe caught RIORegisterBuffer "succeeding" with id
/// 0xffffffff).
const RIO_INVALID_CQ: RIO_CQ = 0;
const RIO_INVALID_RQ: RIO_RQ = 0;
const RIO_INVALID_BUFFERID: RIO_BUFFERID = 0xFFFF_FFFF;

/// R-044: dequeue batch per CQ drain.
const DEQUEUE_BATCH: usize = 1024;

/// Send staging geometry: 64 KiB slots in 2 MiB registered regions.
const STAGING_SLOT: usize = 64 * 1024;
const STAGING_REGION: usize = 2 * 1024 * 1024;
const STAGING_SLOTS_PER_REGION: u32 = (STAGING_REGION / STAGING_SLOT) as u32;

/// CQ growth cap: RIO_MAX_CQ_SIZE is 0x8000000; stay well under it.
const CQ_MAX: u32 = 8 * 1024 * 1024;

/// RIO op ids live in their own slab; this bit keeps them disjoint from
/// the inner IOCP backend's ids (both feed the same `net.ops` map).
const RIO_INDEX_BIT: u32 = 1 << 30;

fn tag(id: OpId) -> OpId {
    OpId { index: id.index | RIO_INDEX_BIT, generation: id.generation }
}

fn untag(id: OpId) -> OpId {
    OpId { index: id.index & !RIO_INDEX_BIT, generation: id.generation }
}

pub(crate) fn is_rio_id(id: OpId) -> bool {
    id.index & RIO_INDEX_BIT != 0
}

/// RequestContext round-trip: OpId <-> u64.
fn ctx_of(id: OpId) -> u64 {
    ((id.index as u64) << 32) | id.generation as u64
}

fn id_of(ctx: u64) -> OpId {
    OpId { index: (ctx >> 32) as u32, generation: ctx as u32 }
}

/// R-020 auto-probe: is the RIO function table resolvable?
pub fn probe_available() -> bool {
    resolve_table_anchored().is_ok() // AnchorSocket drop closes the probe
}

/// A `WSA_FLAG_REGISTERED_IO` socket held open on purpose. mswsock
/// initializes its per-process RIO state when the first REGISTERED_IO
/// socket is created and tears it down when the last one closes; run 4's
/// hardware probe showed EVERY `RIOCreateCompletionQueue` variant (null,
/// event, and IOCP notification) failing WSAEFAULT once no RIO socket was
/// alive — with a fully valid, correctly-sized function table. The
/// backend therefore keeps the table-resolution socket open ("anchor")
/// for its entire lifetime.
struct AnchorSocket(SOCKET);

impl Drop for AnchorSocket {
    fn drop(&mut self) {
        unsafe { closesocket(self.0) };
    }
}

fn resolve_table_anchored() -> io::Result<(RIO_EXTENSION_FUNCTION_TABLE, AnchorSocket)> {
    super::iocp::ensure_winsock();
    unsafe {
        // The canonical RIO pattern (per the SDK samples) resolves the
        // table from a socket created WITH the REGISTERED_IO flag — and
        // keeps that socket open (see AnchorSocket).
        let probe = WSASocketW(
            AF_INET as i32,
            1, // SOCK_STREAM
            IPPROTO_TCP,
            std::ptr::null(),
            0,
            WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO,
        );
        if probe == !0usize {
            return Err(io::Error::from_raw_os_error(WSAGetLastError()));
        }
        let anchor = AnchorSocket(probe);
        let guid: GUID = WSAID_MULTIPLE_RIO;
        let mut table: RIO_EXTENSION_FUNCTION_TABLE = zeroed();
        let mut bytes: u32 = 0;
        let rc = WSAIoctl(
            probe,
            SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
            (&guid as *const GUID).cast(),
            size_of::<GUID>() as u32,
            (&mut table as *mut RIO_EXTENSION_FUNCTION_TABLE).cast(),
            size_of::<RIO_EXTENSION_FUNCTION_TABLE>() as u32,
            &mut bytes,
            std::ptr::null_mut(),
            None,
        );
        if rc == SOCKET_ERROR {
            return Err(io::Error::from_raw_os_error(WSAGetLastError()));
        }
        Ok((table, anchor))
    }
}

/// WSAGetLastError, naming the failing RIO call — hardware validation reports
/// arrive as log files, so every failure must identify its site.
fn wsa_named(call: &'static str) -> io::Error {
    let raw = unsafe { WSAGetLastError() };
    let base = io::Error::from_raw_os_error(raw);
    io::Error::new(base.kind(), format!("{call} failed: {base}"))
}

/// Per-op state: (sends only) the staging slot pinned until the
/// completion is reaped — the kernel reads from it (R-073 discipline
/// applied to staging).
struct RioOp {
    staging: Option<u32>,
}

fn empty_op() -> RioOp {
    RioOp { staging: None }
}

/// One VirtualAlloc'd, RIORegisterBuffer'd send-staging region.
struct StagingRegion {
    ptr: *mut u8,
    id: RIO_BUFFERID,
}

pub struct RioBackend {
    inner: IocpBackend,
    t: RIO_EXTENSION_FUNCTION_TABLE,
    cq: RIO_CQ,
    cq_ledger: CqLedger,
    rq_recv: u32,
    rq_send: u32,
    /// socket -> (RQ, reserved CQ slots). RQ handles die with closesocket.
    rqs: HashMap<SOCKET, (RIO_RQ, u32)>,
    /// Recv-slab regions registered via the trait hook (R-043).
    regions: RegionMap,
    staging_regions: Vec<StagingRegion>,
    staging: StagingLedger,
    /// Stable OVERLAPPED for the CQ's IOCP notification.
    _notify_overlapped: Box<OVERLAPPED>,
    notify_armed: bool,
    /// IOCP-notify CQ creation failed; running on a notification-less CQ.
    /// Parked polls clamp to 1ms while RIO ops are in flight so the drain
    /// stays prompt. Visible via name() == "rio-polling".
    polling_only: bool,
    /// Outstanding RIO requests (drives the polling-only park clamp).
    inflight: u32,
    /// Diagnostics (R-103): KEY_RIO notifications received, and
    /// completions found by the poll-top drain while a notification was
    /// armed (nonzero = notifications are not arriving; the watchdog
    /// park cap is what kept I/O moving).
    stat_notifies: u64,
    stat_watchdog_reaps: u64,
    slab: OpSlab<RioOp>,
    results: Box<[RIORESULT; DEQUEUE_BATCH]>,
    entries: Box<[OVERLAPPED_ENTRY; POLL_BATCH]>,
    /// Keeps mswsock's per-process RIO state alive (see AnchorSocket).
    /// Field drop runs after the explicit `Drop` impl closes the CQ, so
    /// RIO state outlives every RIO handle owned by this backend.
    _anchor: AnchorSocket,
}

// SAFETY: thread-affine by the loop contract (see gil_boundary); raw
// handles/pointers move with the owning loop.
unsafe impl Send for RioBackend {}

impl RioBackend {
    pub fn new(cq_size: u32, rq_recv: u32, rq_send: u32) -> io::Result<Self> {
        let (t, anchor) = resolve_table_anchored()
            .map_err(|e| io::Error::new(e.kind(), format!("RIO unavailable on this system: {e}")))?;
        // Every entry point we rely on must have resolved.
        if t.RIOReceive.is_none()
            || t.RIOSend.is_none()
            || t.RIOCreateCompletionQueue.is_none()
            || t.RIOCreateRequestQueue.is_none()
            || t.RIODequeueCompletion.is_none()
            || t.RIONotify.is_none()
            || t.RIORegisterBuffer.is_none()
            || t.RIODeregisterBuffer.is_none()
            || t.RIOResizeCompletionQueue.is_none()
            || t.RIOCloseCompletionQueue.is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "RIO function table incomplete on this system",
            ));
        }
        let mut inner = IocpBackend::new()?;
        // Accepted connections must be RQ-capable (R-042).
        inner.set_accept_socket_flags(WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO);

        let mut notify_overlapped: Box<OVERLAPPED> = Box::new(unsafe { zeroed() });
        let notify = RIO_NOTIFICATION_COMPLETION {
            Type: RIO_IOCP_COMPLETION,
            Anonymous: RIO_NOTIFICATION_COMPLETION_0 {
                Iocp: RIO_NOTIFICATION_COMPLETION_0_1 {
                    IocpHandle: inner.port_handle() as HANDLE,
                    CompletionKey: KEY_RIO as *mut core::ffi::c_void,
                    Overlapped: (&mut *notify_overlapped as *mut OVERLAPPED).cast(),
                },
            },
        };
        let cq_size = cq_size.clamp(256, CQ_MAX);
        let mut polling_only = false;
        let mut cq = unsafe { (t.RIOCreateCompletionQueue.unwrap())(cq_size, &notify) };
        if cq == RIO_INVALID_CQ {
            let notify_err = wsa_named("RIOCreateCompletionQueue(IOCP-notify)");
            // Fallback: a notification-less CQ (valid per the API — pure
            // RIODequeueCompletion polling). Keeps RIO usable for
            // validation while the notify path is diagnosed
            // (tools/windows: cargo run --example rio_probe).
            cq = unsafe { (t.RIOCreateCompletionQueue.unwrap())(cq_size, std::ptr::null()) };
            if cq == RIO_INVALID_CQ {
                // Name BOTH failures: a null-notify CQ rejecting too means
                // the problem is not the notification struct at all.
                let poll_err = wsa_named("RIOCreateCompletionQueue(null-notify)");
                return Err(io::Error::new(
                    notify_err.kind(),
                    format!("{notify_err}; fallback {poll_err}"),
                ));
            }
            polling_only = true;
        }
        let mut backend = RioBackend {
            inner,
            t,
            cq,
            cq_ledger: CqLedger::new(cq_size, CQ_MAX),
            rq_recv: rq_recv.max(1),
            rq_send: rq_send.max(1),
            rqs: HashMap::new(),
            regions: RegionMap::new(),
            staging_regions: Vec::new(),
            staging: StagingLedger::new(STAGING_SLOTS_PER_REGION),
            _notify_overlapped: notify_overlapped,
            notify_armed: false,
            polling_only,
            inflight: 0,
            stat_notifies: 0,
            stat_watchdog_reaps: 0,
            slab: OpSlab::new(empty_op),
            results: Box::new(unsafe { zeroed() }),
            entries: Box::new(unsafe { zeroed() }),
            _anchor: anchor,
        };
        backend.grow_staging()?;
        Ok(backend)
    }

    fn grow_staging(&mut self) -> io::Result<()> {
        let ptr = unsafe {
            VirtualAlloc(std::ptr::null(), STAGING_REGION, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE)
        };
        if ptr.is_null() {
            let base = io::Error::last_os_error();
            return Err(io::Error::new(base.kind(), format!("VirtualAlloc(staging) failed: {base}")));
        }
        let id = unsafe { (self.t.RIORegisterBuffer.unwrap())(ptr.cast(), STAGING_REGION as u32) };
        if id == RIO_INVALID_BUFFERID {
            let err = wsa_named("RIORegisterBuffer(staging)");
            unsafe { VirtualFree(ptr, 0, MEM_RELEASE) };
            return Err(err);
        }
        self.staging_regions.push(StagingRegion { ptr: ptr.cast(), id });
        self.staging.add_region();
        Ok(())
    }

    fn staging_ptr(&self, slot: u32) -> (*mut u8, RIO_BUFFERID, u32) {
        let (region, idx) = self.staging.locate(slot);
        let r = &self.staging_regions[region as usize];
        let off = idx as usize * STAGING_SLOT;
        (unsafe { r.ptr.add(off) }, r.id, off as u32)
    }

    /// Drain the CQ (R-044 batch). Returns an error only on CQ corruption
    /// (§16: fatal, never silent loss).
    fn drain_cq(&mut self, out: &mut Vec<Completion>) -> io::Result<()> {
        loop {
            let n = unsafe {
                (self.t.RIODequeueCompletion.unwrap())(
                    self.cq,
                    self.results.as_mut_ptr(),
                    DEQUEUE_BATCH as u32,
                )
            };
            if n == RIO_CORRUPT_CQ {
                return Err(io::Error::other("RIO completion queue corrupt (RIO_CORRUPT_CQ)"));
            }
            if n == 0 {
                return Ok(());
            }
            for i in 0..n as usize {
                let r = self.results[i];
                let id = id_of(r.RequestContext);
                let Some((_kind, was_cancelled)) = self.slab.complete(id) else {
                    continue; // stale (generation-checked) — cannot happen absent kernel bugs
                };
                self.inflight = self.inflight.saturating_sub(1);
                if let Some(slot) = self.slab.get(id).and_then(|s| s.data.staging) {
                    self.staging.free(slot);
                }
                self.slab.release(id);
                let os_error = if was_cancelled {
                    WSA_OPERATION_ABORTED as u32
                } else {
                    r.Status as u32
                };
                out.push(Completion::Io { op: tag(id), bytes: r.BytesTransferred, os_error });
            }
            if (n as usize) < DEQUEUE_BATCH {
                return Ok(());
            }
        }
    }
}

impl Drop for RioBackend {
    fn drop(&mut self) {
        unsafe {
            if let Some(close_cq) = self.t.RIOCloseCompletionQueue {
                if self.cq != RIO_INVALID_CQ {
                    close_cq(self.cq);
                }
            }
            if let Some(dereg) = self.t.RIODeregisterBuffer {
                for r in &self.staging_regions {
                    dereg(r.id);
                }
            }
            for r in &self.staging_regions {
                VirtualFree(r.ptr.cast(), 0, MEM_RELEASE);
            }
        }
    }
}

impl IoBackend for RioBackend {
    fn register_socket(&mut self, socket: RawSocket) -> io::Result<()> {
        // Port binding + skip-modes for the IOCP-side ops (probes etc.).
        self.inner.register_socket(socket)?;
        // R-042: attach a request queue. CQ capacity is reserved per RQ,
        // growing the CQ by doubling when needed (R-041).
        let reserve = self.rq_recv + self.rq_send;
        let grow = match self.cq_ledger.plan_reserve(reserve) {
            Ok(g) => g,
            Err(()) => {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "RIO completion queue at maximum capacity",
                ))
            }
        };
        if let Some(new_size) = grow {
            let ok = unsafe { (self.t.RIOResizeCompletionQueue.unwrap())(self.cq, new_size) };
            if ok == 0 {
                return Err(wsa_named("RIOResizeCompletionQueue"));
            }
        }
        let rq = unsafe {
            (self.t.RIOCreateRequestQueue.unwrap())(
                socket,
                self.rq_recv,
                1, // RIO: exactly one data buffer per receive
                self.rq_send,
                1,
                self.cq,
                self.cq,
                std::ptr::null(),
            )
        };
        if rq == RIO_INVALID_RQ {
            // Not RQ-capable (socket lacks WSA_FLAG_REGISTERED_IO — e.g. a
            // netsys-created outbound socket): fall back to inner IOCP ops
            // for this socket. Real resource errors surface on the next
            // registration attempt too, so a silent per-socket fallback is
            // safe here; the mixed mode is by design (see module docs).
            return Ok(());
        }
        self.cq_ledger.commit(reserve, grow);
        self.rqs.insert(socket, (rq, reserve));
        Ok(())
    }

    fn take_accept_socket(&mut self, op: OpId) -> io::Result<RawSocket> {
        self.inner.take_accept_socket(op)
    }

    fn post_accept(&mut self, listener: RawSocket) -> io::Result<OpId> {
        self.inner.post_accept(listener)
    }

    // R-051: named pipes are never RQ-capable (WSA_FLAG_REGISTERED_IO is
    // socket-only) — always the inner IOCP path, same as datagrams below.
    fn register_pipe(&mut self, handle: RawSocket) -> io::Result<()> {
        self.inner.register_pipe(handle)
    }

    fn post_pipe_read(&mut self, handle: RawSocket, buf: *mut u8, len: u32) -> io::Result<OpId> {
        self.inner.post_pipe_read(handle, buf, len)
    }

    fn post_pipe_write(&mut self, handle: RawSocket, data: &[u8]) -> io::Result<OpId> {
        self.inner.post_pipe_write(handle, data)
    }

    fn post_recv(&mut self, socket: RawSocket, buf: *mut u8, len: u32) -> io::Result<OpId> {
        let Some(&(rq, _)) = self.rqs.get(&socket) else {
            return self.inner.post_recv(socket, buf, len);
        };
        // R-043: the pool slab this buffer lives in must be registered
        // (the reactor calls register_buffers on slab growth).
        let Some((cookie, offset)) = self.regions.resolve(buf as usize, len as usize) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "recv buffer is not inside a registered region",
            ));
        };
        let id = self.slab.post(OpKind::Recv);
        self.slab.get_mut(id).unwrap().data = RioOp { staging: None };
        let rbuf =
            RIO_BUF { BufferId: cookie as RIO_BUFFERID, Offset: offset, Length: len };
        let ok = unsafe {
            (self.t.RIOReceive.unwrap())(rq, &rbuf, 1, 0, ctx_of(id) as *const core::ffi::c_void)
        };
        if ok == 0 {
            let err = wsa_named("RIOReceive");
            self.slab.complete(id);
            self.slab.release(id);
            return Err(err);
        }
        self.inflight += 1;
        Ok(tag(id))
    }

    fn post_send(&mut self, socket: RawSocket, bufs: &[IoSlice]) -> io::Result<OpId> {
        let Some(&(rq, _)) = self.rqs.get(&socket) else {
            return self.inner.post_send(socket, bufs);
        };
        // Copy the gather list into one registered staging slot (RIO takes
        // a single buffer per request). Anything beyond the slot is left
        // for the transport's partial-send resumption (R-035 handles short
        // writes identically on every backend).
        let slot = match self.staging.alloc() {
            Some(s) => s,
            None => {
                self.grow_staging()?;
                self.staging.alloc().expect("fresh staging region has free slots")
            }
        };
        let (dst, buf_id, base_off) = self.staging_ptr(slot);
        let mut copied = 0usize;
        for b in bufs {
            if copied >= STAGING_SLOT {
                break;
            }
            let take = (b.len as usize).min(STAGING_SLOT - copied);
            unsafe { std::ptr::copy_nonoverlapping(b.ptr, dst.add(copied), take) };
            copied += take;
        }
        let id = self.slab.post(OpKind::Send);
        self.slab.get_mut(id).unwrap().data = RioOp { staging: Some(slot) };
        let rbuf = RIO_BUF { BufferId: buf_id, Offset: base_off, Length: copied as u32 };
        let ok = unsafe {
            (self.t.RIOSend.unwrap())(rq, &rbuf, 1, 0, ctx_of(id) as *const core::ffi::c_void)
        };
        if ok == 0 {
            let err = wsa_named("RIOSend");
            self.staging.free(slot);
            self.slab.complete(id);
            self.slab.release(id);
            return Err(err);
        }
        self.inflight += 1;
        Ok(tag(id))
    }

    fn post_connect(&mut self, socket: RawSocket, addr: &[u8]) -> io::Result<OpId> {
        self.inner.post_connect(socket, addr)
    }

    // R-058: datagrams ride the inner IOCP surface in the hybrid (RIO
    // datagram RQs are a possible later refinement).
    fn post_recv_from(&mut self, socket: RawSocket, buf: *mut u8, len: u32) -> io::Result<OpId> {
        self.inner.post_recv_from(socket, buf, len)
    }

    fn take_recv_from_addr(&mut self, op: OpId) -> Option<std::net::SocketAddr> {
        self.inner.take_recv_from_addr(op)
    }

    fn post_send_to(
        &mut self,
        socket: RawSocket,
        data: &[u8],
        addr: Option<&std::net::SocketAddr>,
    ) -> io::Result<OpId> {
        self.inner.post_send_to(socket, data, addr)
    }

    fn post_disconnect_reuse(&mut self, socket: RawSocket) -> io::Result<OpId> {
        // Recycled sockets keep their RQ association wrongly; simplest
        // correct behavior on RIO is close-don't-recycle.
        let _ = socket;
        Err(io::Error::new(io::ErrorKind::Unsupported, "socket reuse disabled on RIO"))
    }

    fn cancel(&mut self, op: OpId) -> io::Result<()> {
        if !is_rio_id(op) {
            return self.inner.cancel(op);
        }
        // RIO has no cancellation syscall: mark the op; its completion
        // still arrives (data, error, or closesocket flushing the RQ) and
        // is translated as WSA_OPERATION_ABORTED (R-037 parity).
        self.slab.mark_cancelled(untag(op));
        Ok(())
    }

    fn set_watch(&mut self, socket: RawSocket, readable: bool, writable: bool) -> io::Result<()> {
        // Readiness probes are overlapped ops — unavailable on
        // REGISTERED_IO sockets. Ours are only accept sockets, which the
        // transport layer never watches; user fds lack the flag and work.
        self.inner.set_watch(socket, readable, writable)
    }

    fn detach_socket(&mut self, socket: RawSocket) {
        if let Some((_rq, reserve)) = self.rqs.remove(&socket) {
            // The RQ handle dies with closesocket; its outstanding
            // requests flush through the CQ (slab + staging reaped there).
            self.cq_ledger.release(reserve);
        }
        self.inner.detach_socket(socket);
    }

    fn register_buffers(
        &mut self,
        regions: &mut [(*mut u8, usize, &mut Option<u64>)],
    ) -> io::Result<()> {
        for (ptr, len, cookie) in regions.iter_mut() {
            if cookie.is_some() {
                continue;
            }
            let id = unsafe { (self.t.RIORegisterBuffer.unwrap())((*ptr).cast(), *len as u32) };
            if id == RIO_INVALID_BUFFERID {
                return Err(wsa_named("RIORegisterBuffer(slab)"));
            }
            **cookie = Some(id as u64);
            if !self.regions.insert(*ptr as usize, *len, id as u64) {
                unsafe { (self.t.RIODeregisterBuffer.unwrap())(id) };
                **cookie = None;
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "overlapping buffer region"));
            }
        }
        Ok(())
    }

    fn deregister_buffers(&mut self, cookies: &[u64]) -> io::Result<()> {
        for &c in cookies {
            if self.regions.remove_cookie(c) {
                unsafe { (self.t.RIODeregisterBuffer.unwrap())(c as RIO_BUFFERID) };
            }
        }
        Ok(())
    }

    fn poll(&mut self, out: &mut Vec<Completion>, timeout: Option<Duration>) -> io::Result<usize> {
        let before = out.len();
        self.inner.pre_poll(out);
        // Spin path (R-060/R-041): drain the CQ directly, no notification.
        let drained_before = out.len();
        self.drain_cq(out)?;
        if self.notify_armed && out.len() > drained_before {
            // Completions surfaced by the drain while a notification was
            // armed: the notification path is not delivering (first
            // observed on hardware as a total stall). Counted for
            // stats(); the watchdog park cap below is the safety net.
            self.stat_watchdog_reaps += (out.len() - drained_before) as u64;
        }
        let mut timeout_ms: u32 = match timeout {
            _ if out.len() > before => 0,
            Some(t) => t.as_millis().min(u32::MAX as u128) as u32,
            None => 0,
        };
        if self.polling_only && self.inflight > 0 {
            // No CQ notification: keep parks short so completions are
            // drained promptly (degraded mode, see `polling_only`).
            timeout_ms = timeout_ms.min(1);
        } else if self.inflight > 0 {
            // Watchdog (defense-in-depth after the hardware stall): even
            // in full-notify mode never park unbounded while RIO ops are
            // outstanding — a lost notification then costs <=50ms, not a
            // hang. 20 wakeups/s while idle-with-pending-recvs is noise.
            timeout_ms = timeout_ms.min(50);
        }
        if timeout_ms > 0 && !self.polling_only && !self.notify_armed {
            // Arm exactly when we might park; RIONotify posts to the inner
            // port under KEY_RIO when completions arrive (R-041).
            let rc = unsafe { (self.t.RIONotify.unwrap())(self.cq) };
            if rc == 0 {
                self.notify_armed = true;
            } else {
                // Arming failed: parking blind would strand every CQ
                // completion until an unrelated wakeup. Degrade to
                // polling mode permanently (name() reflects it).
                self.polling_only = true;
                timeout_ms = timeout_ms.min(1);
            }
        }
        let mut n: u32 = 0;
        let ok = unsafe {
            GetQueuedCompletionStatusEx(
                self.inner.port_handle(),
                self.entries.as_mut_ptr(),
                POLL_BATCH as u32,
                &mut n,
                timeout_ms,
                0,
            )
        };
        if ok == 0 {
            let err = unsafe { GetLastError() };
            if err == WAIT_TIMEOUT {
                return Ok(out.len() - before);
            }
            return Err(io::Error::from_raw_os_error(err as i32));
        }
        for i in 0..n as usize {
            let entry = self.entries[i];
            if entry.lpCompletionKey == KEY_RIO {
                self.stat_notifies += 1;
                self.notify_armed = false;
                self.drain_cq(out)?;
            } else {
                self.inner.translate_entry(&entry, out);
            }
        }
        Ok(out.len() - before)
    }

    fn wakeup_handle(&self) -> Arc<dyn Wakeup> {
        self.inner.wakeup_handle()
    }

    fn name(&self) -> &'static str {
        if self.polling_only {
            "rio-polling"
        } else {
            "rio"
        }
    }

    fn diag(&self) -> Option<(u64, u64)> {
        Some((self.stat_notifies, self.stat_watchdog_reaps))
    }
}
