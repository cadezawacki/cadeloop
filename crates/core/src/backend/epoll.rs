//! Linux epoll backend: the IOCP op surface emulated over readiness.
//!
//! Purpose: make cadeloop a *working* drop-in event loop on Linux so the
//! full asyncio surface (transports, sock_*, add_reader) is developed and
//! tested everywhere, and so Linux benchmarks exercise real socket I/O.
//! Windows/IOCP remains the production performance target; this backend
//! favors correctness and a shared L2, with known Linux-specific headroom
//! documented below.
//!
//! Model: `post_*` performs the syscall immediately (fast path — mirrors
//! IOCP's FILE_SKIP_COMPLETION_PORT_ON_SUCCESS inline completions, R-031);
//! on `EWOULDBLOCK`/`EINPROGRESS` the op parks in the fd's slot and epoll
//! interest is (re)computed. `poll` waits on epoll + an eventfd waker,
//! performs the parked syscalls for ready fds, and emits the same
//! `Completion::Io` events IOCP produces. Readiness watches (R-057) are
//! native level-triggered interest emitting `Completion::Ready`.
//!
//! Invariants:
//! * at most ONE parked recv-side and ONE parked send-side op per fd
//!   (the transport layer guarantees this; enforced with debug_asserts);
//! * op payload buffers stay pinned by the op slab until completion, same
//!   contract as IOCP (R-037);
//! * `cancel` completes the parked op with `ECANCELED` at the next poll —
//!   completions are never dropped (state machine parity with IOCP).
//!
//! Perf headroom intentionally left for later (Linux is not the
//! acceptance platform): EPOLLET + always-armed interest would cut
//! `epoll_ctl` churn; `recvmmsg`/`sendmmsg` batching; io_uring backend.

use std::collections::HashMap;
use std::io;
use std::mem::zeroed;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{Completion, IoBackend, IoSlice, RawSocket, Wakeup, MAX_GATHER};
use crate::netsys;
use crate::opslab::{OpId, OpKind, OpSlab};

fn errno() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO)
}

fn cvt(rc: i32) -> io::Result<i32> {
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(rc)
    }
}

/// Payload of a parked (or in-flight) op.
// Variant sizes differ by design: ops live in preallocated fixed-size slab
// slots (R-037 pinning), so the large Send gather array costs nothing per
// op — boxing it would add an allocation to the hot write path.
#[allow(clippy::large_enum_variant)]
enum Pending {
    Accept,
    Recv {
        buf: *mut u8,
        len: u32,
    },
    Send {
        bufs: [IoSlice; MAX_GATHER],
        n: u8,
        done: u32,
    },
    Connect,
    /// R-058: peer address captured into the op; the slot lives until
    /// `take_recv_from_addr` (accept-socket lifecycle).
    RecvFrom {
        buf: *mut u8,
        len: u32,
        from: [u8; 128],
        from_len: u32,
    },
    /// R-058: the datagram is copied in (no caller pinning); UDP sendto
    /// either sends whole or fails — no partial resumption.
    SendTo {
        data: Box<[u8]>,
        to: [u8; 128],
        to_len: u32,
    },
}

struct EpollOp {
    fd: RawFd,
    pending: Pending,
    /// Accept result parked until take_accept_socket.
    accepted: RawFd,
}

fn empty_op() -> EpollOp {
    EpollOp { fd: -1, pending: Pending::Accept, accepted: -1 }
}

#[derive(Default)]
struct FdEntry {
    /// Parked recv-side op (recv or accept).
    read_op: Option<OpId>,
    /// Parked send-side op (send or connect).
    write_op: Option<OpId>,
    watch_r: bool,
    watch_w: bool,
    registered: bool,
    /// Event mask currently installed in the kernel. May be a SUPERSET of
    /// `interest()` between an op completing and its re-post (lazy disarm
    /// — see `sync_interest`), which is what removes the per-message
    /// `epoll_ctl` pair from steady-state connections.
    kernel: u32,
    /// The last recv returned less than the buffer: the socket buffer is
    /// drained, so the next `post_recv` parks without the speculative
    /// inline `recv` (it would EAGAIN). Mirrors the readiness-transport
    /// pattern aiofastnet/uvloop use (ADR-20).
    drained: bool,
}

impl FdEntry {
    fn interest(&self) -> u32 {
        let mut ev = 0u32;
        if self.read_op.is_some() || self.watch_r {
            ev |= libc::EPOLLIN as u32;
        }
        if self.write_op.is_some() || self.watch_w {
            ev |= libc::EPOLLOUT as u32;
        }
        ev
    }
}

struct WakeShared {
    eventfd: RawFd,
    armed: AtomicBool,
}

struct EpollWakeup {
    shared: Arc<WakeShared>,
}

impl Wakeup for EpollWakeup {
    fn wake(&self) {
        // Collapse redundant writes: one un-consumed eventfd tick is enough.
        if !self.shared.armed.swap(true, Ordering::AcqRel) {
            let one: u64 = 1;
            unsafe {
                libc::write(self.shared.eventfd, (&one as *const u64).cast(), 8);
            }
        }
    }
}

pub struct EpollBackend {
    epfd: RawFd,
    wake: Arc<WakeShared>,
    slab: OpSlab<EpollOp>,
    fds: HashMap<RawFd, FdEntry>,
    /// Ops completed inline or cancelled, delivered on the next poll.
    inline_completions: Vec<Completion>,
    events: Vec<libc::epoll_event>,
    pub syscalls_saved_inline: u64,
}

// SAFETY: thread-affine by the loop contract (see gil_boundary); raw fds
// move with the owning loop.
unsafe impl Send for EpollBackend {}

impl EpollBackend {
    pub fn new() -> io::Result<Self> {
        let epfd = cvt(unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) })?;
        let eventfd = cvt(unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) })?;
        let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: eventfd as u64 };
        cvt(unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_ADD, eventfd, &mut ev) })?;
        Ok(EpollBackend {
            epfd,
            wake: Arc::new(WakeShared { eventfd, armed: AtomicBool::new(false) }),
            slab: OpSlab::new(empty_op),
            fds: HashMap::new(),
            inline_completions: Vec::with_capacity(32),
            events: vec![unsafe { zeroed() }; 1024],
            syscalls_saved_inline: 0,
        })
    }

    /// Bring the kernel's event mask in line with `interest()`.
    ///
    /// `lazy=true` (op completion paths): a kernel mask that is a SUPERSET
    /// of the interest is left stale — the transport re-posts within the
    /// same tick, at which point desired == kernel and no `epoll_ctl` runs
    /// at all. If nothing re-posts, the next spurious event disarms it
    /// (`poll` calls back with `lazy=false`). This is the aiofastnet
    /// mirror (ADR-20): steady-state connections keep one persistent
    /// level-triggered registration instead of a DEL/ADD pair per message.
    fn sync_interest(&mut self, fd: RawFd, lazy: bool) -> io::Result<()> {
        let Some(entry) = self.fds.get_mut(&fd) else { return Ok(()) };
        let interest = entry.interest();
        if interest == entry.kernel {
            return Ok(());
        }
        if lazy && interest & !entry.kernel == 0 {
            return Ok(()); // kernel is a superset: leave it armed
        }
        let mut ev = libc::epoll_event { events: interest, u64: fd as u64 };
        if entry.registered {
            if interest == 0 {
                cvt(unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_DEL, fd, &mut ev) })?;
                entry.registered = false;
                entry.kernel = 0;
                self.fds.remove(&fd);
            } else {
                cvt(unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_MOD, fd, &mut ev) })?;
                entry.kernel = interest;
            }
        } else if interest != 0 {
            cvt(unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_ADD, fd, &mut ev) })?;
            entry.registered = true;
            entry.kernel = interest;
        } else {
            self.fds.remove(&fd);
        }
        Ok(())
    }

    /// Park an op on the fd's read or write side. Readiness is per-fd here,
    /// so there is exactly ONE parked slot per side -- a second park would
    /// overwrite the first, stranding its slab entry as permanently
    /// unreachable while still counted live. Callers that maintain a pool
    /// (accepts) must therefore treat WouldBlock as "the pool is full".
    fn park(&mut self, id: OpId, fd: RawFd, write_side: bool) -> io::Result<OpId> {
        let entry = self.fds.entry(fd).or_default();
        let slot = if write_side { &mut entry.write_op } else { &mut entry.read_op };
        if slot.is_some() {
            // Release the slab entry we were about to strand, and report a
            // would-block so the caller stops rather than silently leaking.
            self.slab.complete(id);
            self.slab.release(id);
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        *slot = Some(id);
        self.sync_interest(fd, false)?;
        Ok(id)
    }

    /// Attempt the syscall for a parked op. Returns Some(completion data)
    /// when it finished (success or error), None to stay parked.
    fn attempt(&mut self, id: OpId) -> Option<(u32, u32)> {
        let slot = self.slab.get_mut(id)?;
        let fd = slot.data.fd;
        match &mut slot.data.pending {
            Pending::Recv { buf, len } => {
                let rc = unsafe { libc::recv(fd, (*buf).cast(), *len as usize, 0) };
                if rc >= 0 {
                    Some((rc as u32, 0))
                } else if errno() == libc::EAGAIN {
                    None
                } else {
                    Some((0, errno() as u32))
                }
            }
            Pending::Send { bufs, n, done } => {
                let mut iov: [libc::iovec; MAX_GATHER] = unsafe { zeroed() };
                let mut skip = *done as usize;
                let mut cnt = 0usize;
                for b in bufs.iter().take(*n as usize) {
                    let blen = b.len as usize;
                    if skip >= blen {
                        skip -= blen;
                        continue;
                    }
                    iov[cnt] = libc::iovec {
                        iov_base: unsafe { b.ptr.add(skip) as *mut libc::c_void },
                        iov_len: blen - skip,
                    };
                    skip = 0;
                    cnt += 1;
                }
                if cnt == 0 {
                    return Some((*done, 0));
                }
                let rc = unsafe { libc::writev(fd, iov.as_ptr(), cnt as i32) };
                if rc >= 0 {
                    *done += rc as u32;
                    let total: u32 = bufs.iter().take(*n as usize).map(|b| b.len).sum();
                    if *done >= total {
                        Some((*done, 0))
                    } else {
                        None // partial: stay parked for EPOLLOUT
                    }
                } else if errno() == libc::EAGAIN {
                    None
                } else {
                    Some((*done, errno() as u32))
                }
            }
            Pending::Accept => {
                let rc = unsafe {
                    libc::accept4(
                        fd,
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                        libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                    )
                };
                if rc >= 0 {
                    slot.data.accepted = rc;
                    Some((0, 0))
                } else if errno() == libc::EAGAIN {
                    None
                } else {
                    Some((0, errno() as u32))
                }
            }
            Pending::Connect => {
                let mut err: i32 = 0;
                let mut len = std::mem::size_of::<i32>() as libc::socklen_t;
                let rc = unsafe {
                    libc::getsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_ERROR,
                        (&mut err as *mut i32).cast(),
                        &mut len,
                    )
                };
                if rc < 0 {
                    Some((0, errno() as u32))
                } else if err == 0 {
                    Some((0, 0))
                } else if err == libc::EINPROGRESS || err == libc::EALREADY {
                    None
                } else {
                    Some((0, err as u32))
                }
            }
            Pending::RecvFrom { buf, len, from, from_len } => {
                let mut sl: libc::socklen_t = from.len() as libc::socklen_t;
                let rc = unsafe {
                    libc::recvfrom(fd, (*buf).cast(), *len as usize, 0, from.as_mut_ptr().cast(), &mut sl)
                };
                if rc >= 0 {
                    *from_len = sl as u32;
                    Some((rc as u32, 0))
                } else if errno() == libc::EAGAIN {
                    None
                } else {
                    Some((0, errno() as u32))
                }
            }
            Pending::SendTo { data, to, to_len } => {
                let (to_ptr, sl): (*const libc::sockaddr, libc::socklen_t) = if *to_len == 0 {
                    (std::ptr::null(), 0) // connected-mode send()
                } else {
                    (to.as_ptr().cast(), *to_len as libc::socklen_t)
                };
                let rc = unsafe { libc::sendto(fd, data.as_ptr().cast(), data.len(), 0, to_ptr, sl) };
                if rc >= 0 {
                    Some((rc as u32, 0))
                } else if errno() == libc::EAGAIN {
                    None
                } else {
                    Some((0, errno() as u32))
                }
            }
        }
    }

    /// Run the parked op for one side of a ready fd. Returns whether the
    /// event had a consumer (a parked op on that side), so `poll` can
    /// lazily disarm stale kernel interest only when nothing wanted it.
    fn drive_side(&mut self, fd: RawFd, write_side: bool, out: &mut Vec<Completion>) -> bool {
        let Some(entry) = self.fds.get_mut(&fd) else { return false };
        let op_slot = if write_side { &mut entry.write_op } else { &mut entry.read_op };
        let Some(id) = *op_slot else { return false };
        // Buffer size of a recv op, read before `attempt` releases the slot
        // (feeds the drained-socket heuristic below).
        let recv_len = self.slab.get(id).and_then(|s| match s.data.pending {
            Pending::Recv { len, .. } => Some(len),
            _ => None,
        });
        if let Some((bytes, os_error)) = self.attempt(id) {
            if let Some(entry) = self.fds.get_mut(&fd) {
                let op_slot = if write_side { &mut entry.write_op } else { &mut entry.read_op };
                *op_slot = None;
                // A short read means the socket buffer is drained: the next
                // post_recv should park straight away instead of paying a
                // speculative recv that will EAGAIN (ADR-20).
                if let Some(len) = recv_len {
                    entry.drained = os_error == 0 && bytes < len;
                }
            }
            // Lazy: leave the kernel mask armed — the transport re-posts
            // within this tick and the re-park then costs no epoll_ctl.
            let _ = self.sync_interest(fd, true);
            let kind = self.slab.get(id).map(|s| s.kind);
            self.slab.complete(id);
            match kind {
                // Slot lives until take_accept_socket / take_recv_from_addr.
                Some(OpKind::Accept | OpKind::RecvFrom) if os_error == 0 => {}
                _ => self.slab.release(id),
            }
            out.push(Completion::Io { op: id, bytes, os_error });
        }
        true
    }
}

impl Drop for EpollBackend {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.wake.eventfd);
            libc::close(self.epfd);
        }
    }
}

impl IoBackend for EpollBackend {
    fn register_socket(&mut self, socket: RawSocket) -> io::Result<()> {
        // Registration is lazy (first parked op / watch); just enforce
        // non-blocking mode, which the completion emulation requires.
        let fd = socket as RawFd;
        // Re-register guard, two cases sharing one rule: (a) fd-number
        // reuse — a previous owner's lazily-kept entry outlived its close;
        // (b) same-socket re-register (connect → attach_stream) — the
        // connect op's lazily-kept EPOLLOUT is still armed in the kernel.
        // Reset both views: drop the entry AND best-effort-DEL the kernel
        // registration (ENOENT for case (a), success for case (b)) so the
        // next park's ADD starts from a clean slate.
        if let Some(entry) = self.fds.remove(&fd) {
            if entry.registered {
                let mut ev: libc::epoll_event = unsafe { zeroed() };
                unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_DEL, fd, &mut ev) };
            }
        }
        let flags = cvt(unsafe { libc::fcntl(fd, libc::F_GETFL) })?;
        cvt(unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) })?;
        Ok(())
    }

    fn take_accept_socket(&mut self, op: OpId) -> io::Result<RawSocket> {
        let slot = self
            .slab
            .get_mut(op)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "accept op not live"))?;
        let fd = slot.data.accepted;
        slot.data.accepted = -1;
        self.slab.release(op);
        if fd < 0 {
            return Err(io::Error::new(io::ErrorKind::NotFound, "accept op has no socket"));
        }
        Ok(fd as RawSocket)
    }

    fn post_accept(&mut self, listener: RawSocket) -> io::Result<OpId> {
        let fd = listener as RawFd;
        let id = self.slab.post(OpKind::Accept);
        {
            let s = self.slab.get_mut(id).unwrap();
            s.data = EpollOp { fd, pending: Pending::Accept, accepted: -1 };
        }
        match self.attempt(id) {
            Some((bytes, err)) => {
                self.syscalls_saved_inline += 1;
                // complete_now handles accept-slot retention/teardown.
                self.complete_now_keeping_accept(id, bytes, err);
                Ok(id)
            }
            None => self.park(id, fd, false),
        }
    }

    fn post_recv(&mut self, socket: RawSocket, buf: *mut u8, len: u32) -> io::Result<OpId> {
        let fd = socket as RawFd;
        let id = self.slab.post(OpKind::Recv);
        {
            let s = self.slab.get_mut(id).unwrap();
            s.data = EpollOp { fd, pending: Pending::Recv { buf, len }, accepted: -1 };
        }
        // Drained socket (last recv came up short): the speculative inline
        // recv would EAGAIN — park directly. Level-triggered epoll covers
        // the race where data arrived in between: the next poll reports it
        // immediately (ADR-20).
        if self.fds.get(&fd).is_some_and(|e| e.drained) {
            return self.park(id, fd, false);
        }
        match self.attempt(id) {
            Some((bytes, err)) => {
                self.syscalls_saved_inline += 1;
                if err == 0 {
                    let entry = self.fds.entry(fd).or_default();
                    entry.drained = bytes < len;
                }
                self.slab.complete(id);
                self.slab.release(id);
                self.inline_completions.push(Completion::Io { op: id, bytes, os_error: err });
                Ok(id)
            }
            None => self.park(id, fd, false),
        }
    }

    fn post_send(&mut self, socket: RawSocket, bufs: &[IoSlice]) -> io::Result<OpId> {
        debug_assert!(!bufs.is_empty() && bufs.len() <= MAX_GATHER);
        let fd = socket as RawFd;
        let id = self.slab.post(OpKind::Send);
        {
            let s = self.slab.get_mut(id).unwrap();
            let mut arr: [IoSlice; MAX_GATHER] = [IoSlice { ptr: std::ptr::null(), len: 0 }; MAX_GATHER];
            let n = bufs.len().min(MAX_GATHER);
            arr[..n].copy_from_slice(&bufs[..n]);
            s.data = EpollOp { fd, pending: Pending::Send { bufs: arr, n: n as u8, done: 0 }, accepted: -1 };
        }
        match self.attempt(id) {
            Some((bytes, err)) => {
                self.syscalls_saved_inline += 1;
                self.slab.complete(id);
                self.slab.release(id);
                self.inline_completions.push(Completion::Io { op: id, bytes, os_error: err });
                Ok(id)
            }
            None => self.park(id, fd, true),
        }
    }

    fn post_recv_from(&mut self, socket: RawSocket, buf: *mut u8, len: u32) -> io::Result<OpId> {
        let fd = socket as RawFd;
        let id = self.slab.post(OpKind::RecvFrom);
        {
            let s = self.slab.get_mut(id).unwrap();
            s.data = EpollOp {
                fd,
                pending: Pending::RecvFrom { buf, len, from: [0; 128], from_len: 0 },
                accepted: -1,
            };
        }
        match self.attempt(id) {
            Some((bytes, err)) => {
                self.slab.complete(id);
                if err != 0 {
                    self.slab.release(id);
                }
                self.inline_completions.push(Completion::Io { op: id, bytes, os_error: err });
                Ok(id)
            }
            None => self.park(id, fd, false),
        }
    }

    fn take_recv_from_addr(&mut self, op: OpId) -> Option<std::net::SocketAddr> {
        let addr = self.slab.get(op).and_then(|s| match &s.data.pending {
            Pending::RecvFrom { from, from_len, .. } => netsys::parse_sockaddr(&from[..*from_len as usize]),
            _ => None,
        });
        if self.slab.get(op).is_some() {
            self.slab.release(op);
        }
        addr
    }

    fn post_send_to(
        &mut self,
        socket: RawSocket,
        data: &[u8],
        addr: Option<&std::net::SocketAddr>,
    ) -> io::Result<OpId> {
        let fd = socket as RawFd;
        let id = self.slab.post(OpKind::SendTo);
        {
            let mut to = [0u8; 128];
            let mut to_len = 0u32;
            if let Some(a) = addr {
                let sa = netsys::build_sockaddr(*a);
                to[..sa.len].copy_from_slice(&sa.buf[..sa.len]);
                to_len = sa.len as u32;
            }
            let s = self.slab.get_mut(id).unwrap();
            s.data = EpollOp {
                fd,
                pending: Pending::SendTo { data: data.to_vec().into_boxed_slice(), to, to_len },
                accepted: -1,
            };
        }
        match self.attempt(id) {
            Some((bytes, err)) => {
                self.slab.complete(id);
                self.slab.release(id);
                self.inline_completions.push(Completion::Io { op: id, bytes, os_error: err });
                Ok(id)
            }
            None => self.park(id, fd, true),
        }
    }

    fn post_connect(&mut self, socket: RawSocket, addr: &[u8]) -> io::Result<OpId> {
        let fd = socket as RawFd;
        let rc = unsafe { libc::connect(fd, addr.as_ptr().cast(), addr.len() as libc::socklen_t) };
        let id = self.slab.post(OpKind::Connect);
        {
            let s = self.slab.get_mut(id).unwrap();
            s.data = EpollOp { fd, pending: Pending::Connect, accepted: -1 };
        }
        if rc == 0 {
            self.syscalls_saved_inline += 1;
            self.slab.complete(id);
            self.slab.release(id);
            self.inline_completions.push(Completion::Io { op: id, bytes: 0, os_error: 0 });
            return Ok(id);
        }
        let e = errno();
        if e == libc::EINPROGRESS {
            return self.park(id, fd, true);
        }
        self.slab.complete(id);
        self.slab.release(id);
        self.inline_completions.push(Completion::Io { op: id, bytes: 0, os_error: e as u32 });
        Ok(id)
    }

    fn post_disconnect_reuse(&mut self, socket: RawSocket) -> io::Result<OpId> {
        // No DisconnectEx equivalent; sockets are simply closed. Signal
        // Unsupported so the transport layer takes the close path.
        let _ = socket;
        Err(io::Error::new(io::ErrorKind::Unsupported, "socket reuse is IOCP-only"))
    }

    fn cancel(&mut self, op: OpId) -> io::Result<()> {
        if !self.slab.mark_cancelled(op) {
            return Ok(());
        }
        // Unpark and deliver ECANCELED on the next poll (completion is
        // never dropped — parity with IOCP's CancelIoEx semantics).
        let (fd, write_side, is_accept, accepted) = {
            let slot = self.slab.get(op).expect("cancelled op is live");
            (
                slot.data.fd,
                matches!(slot.data.pending, Pending::Send { .. } | Pending::Connect | Pending::SendTo { .. }),
                matches!(slot.data.pending, Pending::Accept),
                slot.data.accepted,
            )
        };
        if let Some(entry) = self.fds.get_mut(&fd) {
            let op_slot = if write_side { &mut entry.write_op } else { &mut entry.read_op };
            if *op_slot == Some(op) {
                *op_slot = None;
                let _ = self.sync_interest(fd, true);
            }
        }
        if is_accept && accepted >= 0 {
            unsafe { libc::close(accepted) };
        }
        self.slab.complete(op);
        self.slab.release(op);
        self.inline_completions.push(Completion::Io { op, bytes: 0, os_error: libc::ECANCELED as u32 });
        Ok(())
    }

    fn set_watch(&mut self, socket: RawSocket, readable: bool, writable: bool) -> io::Result<()> {
        let fd = socket as RawFd;
        {
            let entry = self.fds.entry(fd).or_default();
            entry.watch_r = readable;
            entry.watch_w = writable;
        }
        // Watches sync EAGERLY. Lazy removal would let an entry outlive
        // remove_reader() + close(); when the kernel recycles the fd
        // number, the next add_reader would see desired == stale kernel
        // mask and silently skip the epoll_ctl the new fd needs. The lazy
        // path is reserved for transport ops, whose teardown always
        // detaches (removing the entry) before close.
        self.sync_interest(fd, false)
    }

    fn detach_socket(&mut self, socket: RawSocket) {
        let fd = socket as RawFd;
        if let Some(entry) = self.fds.remove(&fd) {
            if entry.registered {
                let mut ev: libc::epoll_event = unsafe { zeroed() };
                unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_DEL, fd, &mut ev) };
            }
            for id in [entry.read_op, entry.write_op].into_iter().flatten() {
                self.slab.mark_cancelled(id);
                self.slab.complete(id);
                self.slab.release(id);
                self.inline_completions.push(Completion::Io {
                    op: id,
                    bytes: 0,
                    os_error: libc::ECANCELED as u32,
                });
            }
        }
    }

    fn poll(&mut self, out: &mut Vec<Completion>, timeout: Option<Duration>) -> io::Result<usize> {
        let before = out.len();
        if !self.inline_completions.is_empty() {
            out.append(&mut self.inline_completions);
        }
        let timeout_ms: i32 = if out.len() > before {
            0
        } else {
            match timeout {
                Some(t) => crate::backend::wait_millis(t).min(i32::MAX as u128) as i32,
                None => 0,
            }
        };
        // Fast path: a zero-timeout poll with no fds registered and no
        // pending wakeup has nothing to discover — skip the epoll_wait
        // syscall entirely. This keeps pure-scheduling ticks (call_soon
        // chains, timer cascades) at userspace-only cost; the cross-thread
        // queue is drained by the reactor regardless, and `armed` covers
        // the eventfd (a Wakeup completion carries no payload).
        if timeout_ms == 0 && self.fds.is_empty() && !self.wake.armed.load(Ordering::Acquire) {
            return Ok(out.len() - before);
        }
        let n = loop {
            let rc = unsafe {
                libc::epoll_wait(self.epfd, self.events.as_mut_ptr(), self.events.len() as i32, timeout_ms)
            };
            if rc >= 0 {
                break rc as usize;
            }
            let e = errno();
            if e == libc::EINTR {
                if timeout_ms != 0 {
                    // Let the reactor re-evaluate timers/signals.
                    break 0;
                }
                continue;
            }
            return Err(io::Error::from_raw_os_error(e));
        };
        for i in 0..n {
            let ev = self.events[i];
            let fd = ev.u64 as RawFd;
            if fd == self.wake.eventfd {
                let mut buf: u64 = 0;
                unsafe { libc::read(self.wake.eventfd, (&mut buf as *mut u64).cast(), 8) };
                self.wake.armed.store(false, Ordering::Release);
                out.push(Completion::Wakeup);
                continue;
            }
            let flags = ev.events;
            let read_ready =
                flags & (libc::EPOLLIN as u32 | libc::EPOLLHUP as u32 | libc::EPOLLERR as u32) != 0;
            let write_ready =
                flags & (libc::EPOLLOUT as u32 | libc::EPOLLHUP as u32 | libc::EPOLLERR as u32) != 0;
            let mut read_used = false;
            let mut write_used = false;
            if read_ready {
                read_used = self.drive_side(fd, false, out);
            }
            if write_ready {
                write_used = self.drive_side(fd, true, out);
            }
            // Watches: emit readiness for whatever remains watched.
            if let Some(entry) = self.fds.get(&fd) {
                let wr = entry.watch_r && read_ready;
                let ww = entry.watch_w && write_ready;
                if wr || ww {
                    out.push(Completion::Ready { socket: fd as RawSocket, readable: wr, writable: ww });
                    read_used |= wr;
                    write_used |= ww;
                }
            }
            // A side fired with no consumer: its kernel bit is stale (op
            // completed and was never re-posted, or a watch was removed).
            // Pay the epoll_ctl NOW to quiet it — this is the only place
            // lazily-kept interest is torn down, so level-triggered epoll
            // can never storm.
            if (read_ready && !read_used) || (write_ready && !write_used) {
                let _ = self.sync_interest(fd, false);
            }
        }
        Ok(out.len() - before)
    }

    fn wakeup_handle(&self) -> Arc<dyn Wakeup> {
        Arc::new(EpollWakeup { shared: self.wake.clone() })
    }

    fn name(&self) -> &'static str {
        "epoll-dev"
    }
}

impl EpollBackend {
    /// Inline accept success: keep the slot (holds the accepted fd) until
    /// `take_accept_socket`, mirroring the IOCP accept lifecycle.
    fn complete_now_keeping_accept(&mut self, id: OpId, bytes: u32, os_error: u32) {
        self.slab.complete(id);
        if os_error != 0 {
            if let Some(slot) = self.slab.get(id) {
                let afd = slot.data.accepted;
                if afd >= 0 {
                    unsafe { libc::close(afd) };
                }
            }
            self.slab.release(id);
        }
        self.inline_completions.push(Completion::Io { op: id, bytes, os_error });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::{AsRawFd, IntoRawFd};

    fn drain(b: &mut EpollBackend, timeout: Duration) -> Vec<Completion> {
        let mut out = Vec::with_capacity(64);
        b.poll(&mut out, Some(timeout)).unwrap();
        out
    }

    #[test]
    fn wakeup_interrupts_and_coalesces() {
        let mut b = EpollBackend::new().unwrap();
        let wake = b.wakeup_handle();
        wake.wake();
        wake.wake();
        let out = drain(&mut b, Duration::from_secs(1));
        assert_eq!(out, vec![Completion::Wakeup]);
        assert!(drain(&mut b, Duration::ZERO).is_empty());
    }

    #[test]
    fn accept_recv_send_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let lfd = listener.into_raw_fd();

        let mut b = EpollBackend::new().unwrap();
        b.register_socket(lfd as RawSocket).unwrap();
        let accept_op = b.post_accept(lfd as RawSocket).unwrap();

        let mut client = TcpStream::connect(addr).unwrap();
        let out = drain(&mut b, Duration::from_secs(2));
        let accepted = match out.as_slice() {
            [Completion::Io { op, os_error: 0, .. }] => {
                assert_eq!(*op, accept_op);
                b.take_accept_socket(*op).unwrap()
            }
            other => panic!("unexpected completions: {other:?}"),
        };
        b.register_socket(accepted).unwrap();

        // Parked recv completes when the client writes.
        let mut buf = [0u8; 64];
        let recv_op = b.post_recv(accepted, buf.as_mut_ptr(), buf.len() as u32).unwrap();
        assert!(drain(&mut b, Duration::from_millis(50)).is_empty(), "recv must park");
        client.write_all(b"ping").unwrap();
        let out = drain(&mut b, Duration::from_secs(2));
        assert_eq!(out, vec![Completion::Io { op: recv_op, bytes: 4, os_error: 0 }]);
        assert_eq!(&buf[..4], b"ping");

        // Gather send: two slices arrive concatenated (inline fast path).
        let (a, z) = (b"he".to_vec(), b"llo".to_vec());
        let slices = [IoSlice { ptr: a.as_ptr(), len: 2 }, IoSlice { ptr: z.as_ptr(), len: 3 }];
        b.post_send(accepted, &slices).unwrap();
        let out = drain(&mut b, Duration::from_secs(1));
        assert!(matches!(out.as_slice(), [Completion::Io { bytes: 5, os_error: 0, .. }]));
        let mut got = [0u8; 5];
        client.read_exact(&mut got).unwrap();
        assert_eq!(&got, b"hello");
        unsafe { libc::close(accepted as RawFd) };
    }

    #[test]
    fn steady_state_pingpong_keeps_lazy_interest_correct() {
        // The ADR-20 fast path: after a parked recv completes, the next
        // post_recv parks without a speculative syscall (drained flag) and
        // without touching epoll_ctl (kernel mask stays armed) — and data
        // must still be delivered reliably across many messages.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let lfd = listener.into_raw_fd();
        let mut b = EpollBackend::new().unwrap();
        b.register_socket(lfd as RawSocket).unwrap();
        b.post_accept(lfd as RawSocket).unwrap();
        let mut client = TcpStream::connect(addr).unwrap();
        let out = drain(&mut b, Duration::from_secs(2));
        let accepted = match out.as_slice() {
            [Completion::Io { op, os_error: 0, .. }] => b.take_accept_socket(*op).unwrap(),
            other => panic!("{other:?}"),
        };
        b.register_socket(accepted).unwrap();

        let mut buf = [0u8; 64 * 1024];
        let inline_before = b.syscalls_saved_inline;
        for i in 0..50u8 {
            let op = b.post_recv(accepted, buf.as_mut_ptr(), buf.len() as u32).unwrap();
            client.write_all(&[i; 8]).unwrap();
            let out = drain(&mut b, Duration::from_secs(2));
            assert_eq!(out, vec![Completion::Io { op, bytes: 8, os_error: 0 }]);
            assert_eq!(&buf[..8], &[i; 8]);
        }
        // After the first short read the drained flag suppresses every
        // speculative inline recv: at most the very first post attempts.
        assert!(
            b.syscalls_saved_inline - inline_before <= 1,
            "speculative recvs not suppressed: {}",
            b.syscalls_saved_inline - inline_before
        );
        unsafe { libc::close(accepted as RawFd) };
    }

    #[test]
    fn full_buffer_read_keeps_inline_attempt() {
        // Streaming pattern: a read that FILLS the buffer clears the
        // drained flag, so the next post_recv drains the socket inline.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let lfd = listener.into_raw_fd();
        let mut b = EpollBackend::new().unwrap();
        b.register_socket(lfd as RawSocket).unwrap();
        b.post_accept(lfd as RawSocket).unwrap();
        let mut client = TcpStream::connect(addr).unwrap();
        let out = drain(&mut b, Duration::from_secs(2));
        let accepted = match out.as_slice() {
            [Completion::Io { op, os_error: 0, .. }] => b.take_accept_socket(*op).unwrap(),
            other => panic!("{other:?}"),
        };
        b.register_socket(accepted).unwrap();

        let mut buf = [0u8; 4];
        // Park, then send 10 bytes: first completion fills the 4-byte
        // buffer (bytes == len -> NOT drained).
        let op = b.post_recv(accepted, buf.as_mut_ptr(), 4).unwrap();
        client.write_all(&[7u8; 10]).unwrap();
        let out = drain(&mut b, Duration::from_secs(2));
        assert_eq!(out, vec![Completion::Io { op, bytes: 4, os_error: 0 }]);
        // Next post must complete INLINE (drained flag clear, data queued).
        let before = b.syscalls_saved_inline;
        let op2 = b.post_recv(accepted, buf.as_mut_ptr(), 4).unwrap();
        assert_eq!(b.syscalls_saved_inline, before + 1, "inline attempt suppressed wrongly");
        let out = drain(&mut b, Duration::ZERO);
        assert_eq!(out, vec![Completion::Io { op: op2, bytes: 4, os_error: 0 }]);
        unsafe { libc::close(accepted as RawFd) };
    }

    #[test]
    fn recv_reports_peer_close_as_zero_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let lfd = listener.into_raw_fd();
        let mut b = EpollBackend::new().unwrap();
        b.register_socket(lfd as RawSocket).unwrap();
        b.post_accept(lfd as RawSocket).unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let out = drain(&mut b, Duration::from_secs(2));
        let accepted = match out.as_slice() {
            [Completion::Io { op, os_error: 0, .. }] => b.take_accept_socket(*op).unwrap(),
            other => panic!("{other:?}"),
        };
        b.register_socket(accepted).unwrap();
        let mut buf = [0u8; 16];
        let op = b.post_recv(accepted, buf.as_mut_ptr(), 16).unwrap();
        drop(client); // orderly shutdown
        let out = drain(&mut b, Duration::from_secs(2));
        assert_eq!(out, vec![Completion::Io { op, bytes: 0, os_error: 0 }]);
        unsafe { libc::close(accepted as RawFd) };
    }

    #[test]
    fn cancel_delivers_ecanceled() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let lfd = listener.into_raw_fd();
        let mut b = EpollBackend::new().unwrap();
        b.register_socket(lfd as RawSocket).unwrap();
        let op = b.post_accept(lfd as RawSocket).unwrap();
        b.cancel(op).unwrap();
        b.cancel(op).unwrap(); // idempotent
        let out = drain(&mut b, Duration::from_millis(50));
        assert_eq!(out, vec![Completion::Io { op, bytes: 0, os_error: libc::ECANCELED as u32 }]);
    }

    #[test]
    fn connect_completes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut b = EpollBackend::new().unwrap();
        let fd =
            cvt(unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_NONBLOCK, 0) }).unwrap();
        b.register_socket(fd as RawSocket).unwrap();
        let sockaddr = match addr {
            std::net::SocketAddr::V4(v4) => {
                let mut sa: libc::sockaddr_in = unsafe { zeroed() };
                sa.sin_family = libc::AF_INET as u16;
                sa.sin_port = v4.port().to_be();
                sa.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
                sa
            }
            _ => unreachable!(),
        };
        let bytes = unsafe {
            std::slice::from_raw_parts(
                (&sockaddr as *const libc::sockaddr_in).cast::<u8>(),
                std::mem::size_of::<libc::sockaddr_in>(),
            )
        };
        let op = b.post_connect(fd as RawSocket, bytes).unwrap();
        let out = drain(&mut b, Duration::from_secs(2));
        assert_eq!(out, vec![Completion::Io { op, bytes: 0, os_error: 0 }]);
        let _server_side = listener.accept().unwrap();
        unsafe { libc::close(fd) };
    }

    #[test]
    fn watch_emits_level_triggered_readiness() {
        let (a, z) = std::os::unix::net::UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        let afd = a.as_raw_fd();
        let mut b = EpollBackend::new().unwrap();
        b.set_watch(afd as RawSocket, true, false).unwrap();
        assert!(drain(&mut b, Duration::from_millis(30)).is_empty());
        (&z).write_all(b"x").unwrap();
        let out = drain(&mut b, Duration::from_secs(1));
        assert_eq!(
            out,
            vec![Completion::Ready { socket: afd as RawSocket, readable: true, writable: false }]
        );
        // Level-triggered: still ready next poll.
        let out = drain(&mut b, Duration::from_millis(100));
        assert_eq!(out.len(), 1);
        // Unwatch stops delivery.
        b.set_watch(afd as RawSocket, false, false).unwrap();
        assert!(drain(&mut b, Duration::from_millis(30)).is_empty());
        drop((a, z));
    }

    #[test]
    fn detach_cancels_parked_ops() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let lfd = listener.into_raw_fd();
        let mut b = EpollBackend::new().unwrap();
        b.register_socket(lfd as RawSocket).unwrap();
        let op = b.post_accept(lfd as RawSocket).unwrap();
        b.detach_socket(lfd as RawSocket);
        let out = drain(&mut b, Duration::from_millis(30));
        assert_eq!(out, vec![Completion::Io { op, bytes: 0, os_error: libc::ECANCELED as u32 }]);
        unsafe { libc::close(lfd) };
    }
}
