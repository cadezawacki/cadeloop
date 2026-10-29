//! IOCP backend (R-030..R-038, R-057).
//!
//! Techniques implemented per spec:
//!
//! * R-030: `GetQueuedCompletionStatusEx`, batch 256, one call per tick.
//! * R-031: `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | FILE_SKIP_SET_EVENT_ON_HANDLE`
//!   on every socket; synchronous returns are surfaced as inline
//!   completions (never ALSO reaped from the port). Non-IFS providers
//!   (LSPs) detected via `SO_PROTOCOL_INFOW` / `XP1_IFS_HANDLES`, in which
//!   case the skip modes are not applied for that socket. What is known
//!   about a socket is keyed by its handle VALUE, which Windows recycles,
//!   so a fresh association discards it (see `register_socket`); and a
//!   slot retired without a port completion is quarantined until the port
//!   has been drained once, so a packet the kernel queued anyway can never
//!   complete a later op that reused the slot (`iocp_util::Quarantine`).
//! * R-032: `AcceptEx` + `SO_UPDATE_ACCEPT_CONTEXT`; the accept POOL
//!   (pre-posted, replenish-on-completion) lives in the M1 listener
//!   transport — this layer provides the primitive.
//! * R-033: `DisconnectEx(TF_REUSE_SOCKET)` + free-socket pool (cap 4096,
//!   keyed by address family/type/protocol) consumed by accept posts.
//! * R-034: `ConnectEx` with bind-before-connect + `SO_UPDATE_CONNECT_CONTEXT`.
//! * R-035: scatter/gather `WSASend`, up to 16 `WSABUF`s.
//! * R-037: per-op `OVERLAPPED` pinned in the [`OpSlab`]; `CancelIoEx`
//!   tolerating `ERROR_NOT_FOUND`; slots recycle only after their
//!   completion is reaped, and the backend does not free the slab while the
//!   kernel still owns any of it (`Drop` cancels and drains). A completion's
//!   status is decoded from its OVERLAPPED, never by asking a handle that
//!   may already be closed.
//! * R-038: `TCP_NODELAY`, optional `SIO_LOOPBACK_FAST_PATH`, optional
//!   `TCP_FASTOPEN` via [`prepare_conn_socket`] / [`prepare_listener`].
//! * R-057: readiness watches (add_reader/add_writer and the sock_* surface)
//!   are `IOCTL_AFD_POLL` requests issued through a socket of our own. The
//!   watched socket is never associated with the port, nothing is read
//!   from it or written to it, and listening and connecting sockets report
//!   real accept and connect readiness.
//!
//! `TransmitFile` (R-036) lands with its transport (M4).

#![allow(clippy::missing_safety_doc)]

use std::ffi::c_void;
use std::io;
use std::mem::{size_of, zeroed};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once, OnceLock};
use std::time::{Duration, Instant};

use rustc_hash::{FxHashMap, FxHashSet};
use windows_sys::core::GUID;
use windows_sys::Wdk::System::IO::NtDeviceIoControlFile;
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, RtlNtStatusToDosError, ERROR_IO_PENDING, ERROR_MORE_DATA, ERROR_NOT_FOUND,
    HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT,
};
use windows_sys::Win32::Networking::WinSock::{
    bind, closesocket, getsockopt, setsockopt, WSAGetLastError, WSAIoctl, WSARecv, WSARecvFrom, WSASend,
    WSASendTo, WSASocketW, WSAStartup, AF_INET, AF_INET6, INVALID_SOCKET, IPPROTO_TCP, LPFN_ACCEPTEX,
    LPFN_CONNECTEX, LPFN_DISCONNECTEX, SIO_BASE_HANDLE, SIO_BSP_HANDLE_POLL, SIO_BSP_HANDLE_SELECT,
    SIO_GET_EXTENSION_FUNCTION_POINTER, SIO_LOOPBACK_FAST_PATH, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6,
    SOCKADDR_STORAGE, SOCKET, SOCKET_ERROR, SOCK_STREAM, SOL_SOCKET, SO_PROTOCOL_INFOW,
    SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT, TCP_FASTOPEN, TCP_NODELAY, TF_REUSE_SOCKET, WSABUF,
    WSADATA, WSAEINVAL, WSAEMSGSIZE, WSAENOTSOCK, WSAID_ACCEPTEX, WSAID_CONNECTEX, WSAID_DISCONNECTEX,
    WSAPROTOCOL_INFOW, WSA_FLAG_OVERLAPPED, WSA_IO_PENDING, XP1_IFS_HANDLES,
};
use windows_sys::Win32::Storage::FileSystem::{ReadFile, SetFileCompletionNotificationModes, WriteFile};
use windows_sys::Win32::System::IO::{
    CancelIoEx, CreateIoCompletionPort, GetQueuedCompletionStatusEx, PostQueuedCompletionStatus,
    IO_STATUS_BLOCK, OVERLAPPED, OVERLAPPED_ENTRY,
};

use super::iocp_util::{
    afd_outcome, afd_poll_events, afd_reported_events, get_or_try_init, nt_error, nt_success,
    ntstatus_to_wsa, overlapped_status, PollOutcome, Quarantine, SocketPool, IOCTL_AFD_POLL,
    STATUS_BUFFER_OVERFLOW, STATUS_PENDING,
};
use super::{Completion, IoBackend, IoSlice, RawSocket, Wakeup, MAX_GATHER};
use crate::netsys;
use crate::opslab::{OpId, OpKind, OpSlab, OpState};

// FILE_SKIP_* constants live in FileSystem in some windows-sys revisions and
// are plain u8 flags; define locally to be revision-proof (values are ABI).
const FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: u8 = 0x1;
const FILE_SKIP_SET_EVENT_ON_HANDLE: u8 = 0x2;

/// Completion keys distinguishing wakeups from socket I/O. KEY_RIO marks
/// the RIO completion queue's IOCP notification (rio.rs).
const KEY_IO: usize = 1;
const KEY_WAKEUP: usize = 2;
pub(crate) const KEY_RIO: usize = 3;
/// The quarantine's drain sentinel (`Quarantine::sentinel_wanted`); its
/// generation rides in the byte count.
const KEY_DRAIN: usize = 4;

/// R-030: dequeue batch size.
pub(crate) const POLL_BATCH: usize = 256;

/// R-033: free-socket pool cap.
const SOCKET_POOL_CAP: usize = 4096;

/// AcceptEx address buffer: local + remote, each sizeof(SOCKADDR_STORAGE)+16.
const ACCEPT_ADDR_LEN: u32 = size_of::<SOCKADDR_STORAGE>() as u32 + 16;
const ACCEPT_BUF_LEN: usize = 2 * ACCEPT_ADDR_LEN as usize;

/// One WriteFile's worth: the call takes a DWORD length, and a pipe write
/// truncated to `len mod 2^32` was silently short. Bigger writes go out in
/// parts through the transport's short-write handling.
const MAX_PIPE_WRITE: usize = 1 << 30;

/// How long teardown waits for cancelled ops to come back before it gives
/// up and leaks the slab rather than free memory the kernel still owns.
/// Cancelled socket and pipe requests complete in microseconds; the bound
/// only matters for a request that ignores cancellation.
pub(crate) const DROP_DRAIN_BUDGET: Duration = Duration::from_secs(1);

fn wsa_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}

fn win_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { GetLastError() } as i32)
}

pub(crate) fn ensure_winsock() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| unsafe {
        let mut data: WSADATA = zeroed();
        let rc = WSAStartup(0x0202, &mut data);
        assert_eq!(rc, 0, "WSAStartup failed: {rc}");
    });
}

/// The Winsock error a failed socket completion reports (R-037): the code
/// `WSAGetOverlappedResult` would have given, from the NTSTATUS alone.
fn socket_error(status: i32) -> u32 {
    if nt_success(status) {
        return 0;
    }
    ntstatus_to_wsa(status).unwrap_or_else(|| unsafe { RtlNtStatusToDosError(status) })
}

/// The Win32 error a failed pipe completion reports: what
/// `GetOverlappedResult` would have given. STATUS_BUFFER_OVERFLOW is a
/// message-mode pipe's "the message is longer than the buffer"
/// (ERROR_MORE_DATA): the buffer is full and valid and the rest of the
/// message comes with the next read, so for a byte-stream transport it is
/// a successful read.
fn pipe_error(status: i32) -> u32 {
    if nt_success(status) || status == STATUS_BUFFER_OVERFLOW {
        return 0;
    }
    unsafe { RtlNtStatusToDosError(status) }
}

/// Extension function pointers (AcceptEx/ConnectEx/DisconnectEx), resolved
/// once per process from a throwaway socket via
/// `SIO_GET_EXTENSION_FUNCTION_POINTER`. Pointers are provider-global for
/// the base MSAFD provider; per-socket re-resolution is unnecessary absent
/// LSPs (which R-031 detection flags separately).
struct ExtFns {
    accept_ex: LPFN_ACCEPTEX,
    connect_ex: LPFN_CONNECTEX,
    disconnect_ex: LPFN_DISCONNECTEX,
}

unsafe fn load_ext_fn<T>(sock: SOCKET, guid: &GUID) -> io::Result<T> {
    let mut func: Option<T> = None;
    let mut bytes: u32 = 0;
    let rc = unsafe {
        WSAIoctl(
            sock,
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            (guid as *const GUID).cast(),
            size_of::<GUID>() as u32,
            (&mut func as *mut Option<T>).cast(),
            size_of::<Option<T>>() as u32,
            &mut bytes,
            std::ptr::null_mut(),
            None,
        )
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    func.ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "extension fn resolved to NULL"))
}

fn resolve_ext_fns() -> io::Result<ExtFns> {
    ensure_winsock();
    unsafe {
        let probe =
            WSASocketW(AF_INET as i32, SOCK_STREAM, IPPROTO_TCP, std::ptr::null(), 0, WSA_FLAG_OVERLAPPED);
        if probe == INVALID_SOCKET {
            return Err(wsa_error());
        }
        let result = (|| {
            Ok(ExtFns {
                accept_ex: Some(load_ext_fn::<
                    unsafe extern "system" fn(
                        SOCKET,
                        SOCKET,
                        *mut c_void,
                        u32,
                        u32,
                        u32,
                        *mut u32,
                        *mut OVERLAPPED,
                    ) -> i32,
                >(probe, &WSAID_ACCEPTEX)?),
                connect_ex: Some(load_ext_fn::<
                    unsafe extern "system" fn(
                        SOCKET,
                        *const SOCKADDR,
                        i32,
                        *const c_void,
                        u32,
                        *mut u32,
                        *mut OVERLAPPED,
                    ) -> i32,
                >(probe, &WSAID_CONNECTEX)?),
                disconnect_ex: Some(load_ext_fn::<
                    unsafe extern "system" fn(SOCKET, *mut OVERLAPPED, u32, u32) -> i32,
                >(probe, &WSAID_DISCONNECTEX)?),
            })
        })();
        closesocket(probe);
        result
    }
}

/// Only success is cached: the throwaway socket can be refused transiently
/// (a descriptor limit at the first accept), and caching that `Err` disabled
/// AcceptEx/ConnectEx for the rest of the process.
fn ext_fns() -> io::Result<&'static ExtFns> {
    static FNS: OnceLock<ExtFns> = OnceLock::new();
    get_or_try_init(&FNS, resolve_ext_fns)
}

/// `AFD_POLL_HANDLE_INFO` / `AFD_POLL_INFO` (afd.sys; layout as libuv,
/// mio and wepoll declare it). One handle per request.
#[repr(C)]
#[derive(Clone, Copy)]
struct AfdPollHandleInfo {
    handle: HANDLE,
    events: u32,
    status: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct AfdPollInfo {
    timeout: i64,
    number_of_handles: u32,
    exclusive: u32,
    handles: [AfdPollHandleInfo; 1],
}

/// Per-op native payload. `overlapped` MUST stay the first field
/// (`repr(C)`): completions hand back `*OVERLAPPED`, which we cast back to
/// `*IocpOp` to recover the `OpId` (the generation check in the slab
/// rejects stale pointers).
#[repr(C)]
pub struct IocpOp {
    overlapped: OVERLAPPED,
    id: OpId,
    socket: SOCKET,
    /// Accept only: the pre-created accept socket.
    accept_socket: SOCKET,
    /// RecvFrom only: length the kernel wrote into `addr_buf` (R-058).
    from_len: i32,
    /// The op finished without a port completion (inline success under
    /// skip mode, or a synchronous failure): its slot is retired through
    /// the quarantine, never straight to the freelist.
    inline: bool,
    /// Poll only: the AFD_POLL_INFO request, which the I/O manager also
    /// copies the result back into at completion (METHOD_BUFFERED), so it
    /// is pinned until the completion is reaped like the OVERLAPPED.
    afd: AfdPollInfo,
    /// SendTo only: the copied-in datagram (kernel reads it until the
    /// completion is reaped).
    dgram: Vec<u8>,
    /// Send only: gather list (R-035). Kernel reads these until completion;
    /// pinned because slab slots never move (R-037).
    wsabufs: [WSABUF; MAX_GATHER],
    /// Accept only: local+remote address block for GetAcceptExSockaddrs.
    addr_buf: [u8; ACCEPT_BUF_LEN],
}

fn empty_op() -> IocpOp {
    IocpOp {
        // SAFETY: all-zero is a valid representation for these C structs
        // (the Vec below is NOT zeroable and is constructed normally).
        overlapped: unsafe { zeroed() },
        // No slot's: a stray packet pointing into a slot never posted
        // since its chunk was allocated resolves to nothing, rather than to
        // slot 0 -- which, at its own generation 0, may be live.
        id: OpId { index: u32::MAX, generation: 0 },
        socket: 0,
        accept_socket: 0,
        from_len: 0,
        inline: false,
        afd: unsafe { zeroed() },
        dgram: Vec::new(),
        wsabufs: unsafe { zeroed() },
        addr_buf: [0; ACCEPT_BUF_LEN],
    }
}

/// One readiness watch (R-057), keyed by the socket's handle value.
struct Watch {
    /// The directions `set_watch` asked for.
    read: bool,
    write: bool,
    /// The socket's AFD base handle: what a poll names.
    base: SOCKET,
    /// The poll in flight, if any, and the events it asked for. At most
    /// one per watch; the entry outlives the watch until it is reaped.
    poll: Option<OpId>,
    polled: u32,
    /// Already in `watch_rearm`.
    queued: bool,
    /// The socket was closed under the watch, or AFD refused the poll:
    /// never poll this base again. A later `set_watch` re-learns the base.
    dead: bool,
}

/// The handle IOCTL_AFD_POLL requests are issued on (R-057).
///
/// Any AFD handle can carry a poll that names any other AFD socket. Issuing
/// them through a socket of our own -- associated with the port, as
/// libuv's "peer socket" is -- means the watched socket never is: a
/// Python-owned socket keeps no tie to this loop, so it can be used with
/// another loop afterwards and closing it needs nothing from us.
struct AfdHelper {
    /// The socket we own and close.
    socket: SOCKET,
    /// Its AFD base handle: what the IOCTLs and their cancellations name.
    base: SOCKET,
}

struct PortHandle(HANDLE);
// SAFETY: an IOCP handle is thread-safe by OS contract (PQCS/GQCS are the
// documented cross-thread mechanism).
unsafe impl Send for PortHandle {}
unsafe impl Sync for PortHandle {}

impl Drop for PortHandle {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.0) };
    }
}

struct IocpWakeup {
    port: Arc<PortHandle>,
    posts: AtomicU64,
}

impl Wakeup for IocpWakeup {
    fn wake(&self) {
        self.posts.fetch_add(1, Ordering::Relaxed);
        // R-022: PostQueuedCompletionStatus as the cross-thread wakeup.
        // Failure is benign post-close; nothing to do about it here.
        unsafe {
            PostQueuedCompletionStatus(self.port.0, 0, KEY_WAKEUP, std::ptr::null_mut());
        }
    }
}

pub struct IocpBackend {
    port: Arc<PortHandle>,
    slab: OpSlab<IocpOp>,
    /// Pre-allocated entry buffer for GetQueuedCompletionStatusEx (R-030).
    entries: Box<[OVERLAPPED_ENTRY; POLL_BATCH]>,
    /// R-031: synchronous successes surfaced inline, drained before the
    /// next kernel dequeue. Also counts toward stats (syscalls saved).
    inline_completions: Vec<Completion>,
    pub syscalls_saved_inline: u64,
    /// Slots retired without a port completion, awaiting a drained port.
    quarantine: Quarantine<OpId>,
    /// R-033: sockets recycled via DisconnectEx(TF_REUSE_SOCKET).
    free_sockets: SocketPool<SOCKET>,
    /// Listener -> protocol info (for creating matching accept sockets).
    listener_info: FxHashMap<SOCKET, WSAPROTOCOL_INFOW>,
    /// Accepts that completed but whose socket `take_accept_socket` has
    /// not collected yet: closed by teardown if it never does.
    untaken_accepts: FxHashSet<OpId>,
    /// R-057 watches by socket; `watch_rearm` holds those whose poll needs
    /// (re)submitting at the next poll (level-trigger cadence).
    watches: FxHashMap<SOCKET, Watch>,
    watch_rearm: Vec<SOCKET>,
    afd: Option<AfdHelper>,
    /// The signal-wakeup fd (R-052): watched, but not I/O interest --
    /// see has_io_interest.
    wake_only: Option<RawSocket>,
    /// Sockets where FILE_SKIP_COMPLETION_PORT_ON_SUCCESS is active.
    /// Synchronous returns may ONLY be handled inline for these (R-031);
    /// for LSP/non-IFS sockets the port still queues a completion even on
    /// immediate success, and treating it inline would double-complete.
    skip_ok: FxHashSet<SOCKET>,
    /// WSASocketW flags for accept sockets. The RIO hybrid adds
    /// WSA_FLAG_REGISTERED_IO so accepted connections are RQ-capable.
    accept_socket_flags: u32,
    /// Sockets this backend associated with its port and has not since
    /// seen detached or closed. It decides `register_socket`'s
    /// ERROR_INVALID_PARAMETER case: "already associated with MY port" (an
    /// attach after tcp_connect, a recycled socket) versus with SOMEONE
    /// ELSE's (a socket that crossed a process boundary, or one another
    /// loop owns), where proceeding would deliver our OVERLAPPED pointers
    /// to a port that cannot interpret them (ADR-25). Only live entries
    /// count: a history that is never pruned let a recycled value pass as
    /// ours.
    associated: FxHashSet<SOCKET>,
    /// ADR-25: completions dequeued carrying an OVERLAPPED outside our slab
    /// (another process's op on a handle bound to our port). Never expected
    /// to be non-zero; surfaced so it is visible rather than silent.
    pub foreign_completions: u64,
    /// Completions for a slot that was not in flight: a packet the kernel
    /// queued for an op already retired inline (see `quarantine`). Dropped
    /// rather than misattributed; never expected to be non-zero.
    pub dropped_completions: u64,
}

// SAFETY: thread-affine by loop contract; raw handles are moved with it.
unsafe impl Send for IocpBackend {}

/// A fresh overlapped, non-inheritable socket (the netsys::create_tcp rule).
fn new_socket(af: i32, ty: i32, proto: i32) -> io::Result<SOCKET> {
    let flags = WSA_FLAG_OVERLAPPED | netsys::WSA_FLAG_NO_HANDLE_INHERIT;
    let s = unsafe { WSASocketW(af, ty, proto, std::ptr::null(), 0, flags) };
    if s != INVALID_SOCKET {
        return Ok(s);
    }
    if unsafe { WSAGetLastError() } != WSAEINVAL {
        return Err(wsa_error());
    }
    let s = unsafe { WSASocketW(af, ty, proto, std::ptr::null(), 0, WSA_FLAG_OVERLAPPED) };
    if s == INVALID_SOCKET {
        return Err(wsa_error());
    }
    netsys::clear_handle_inherit(s);
    Ok(s)
}

fn query_base(socket: SOCKET, ioctl: u32) -> io::Result<SOCKET> {
    let mut base: SOCKET = INVALID_SOCKET;
    let mut bytes: u32 = 0;
    let rc = unsafe {
        WSAIoctl(
            socket,
            ioctl,
            std::ptr::null(),
            0,
            (&mut base as *mut SOCKET).cast(),
            size_of::<SOCKET>() as u32,
            &mut bytes,
            std::ptr::null_mut(),
            None,
        )
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    if base == INVALID_SOCKET || base == 0 {
        return Err(io::Error::from_raw_os_error(WSAENOTSOCK));
    }
    Ok(base)
}

/// The AFD socket under a (possibly layered) socket: what a poll must name.
/// SIO_BASE_HANDLE is not supposed to be interceptable by an LSP, but at
/// least one known LSP breaks it; mio falls back to these and trusts an
/// answer only when it differs from the socket itself.
fn base_socket(socket: SOCKET) -> io::Result<SOCKET> {
    let first = query_base(socket, SIO_BASE_HANDLE);
    if first.is_ok() {
        return first;
    }
    for ioctl in [SIO_BSP_HANDLE_SELECT, SIO_BSP_HANDLE_POLL] {
        if let Ok(b) = query_base(socket, ioctl) {
            if b != socket {
                return Ok(b);
            }
        }
    }
    first
}

fn protocol_info(socket: SOCKET) -> io::Result<WSAPROTOCOL_INFOW> {
    let mut info: WSAPROTOCOL_INFOW = unsafe { zeroed() };
    let mut len = size_of::<WSAPROTOCOL_INFOW>() as i32;
    let rc = unsafe {
        getsockopt(
            socket,
            SOL_SOCKET,
            SO_PROTOCOL_INFOW,
            (&mut info as *mut WSAPROTOCOL_INFOW).cast(),
            &mut len,
        )
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    Ok(info)
}

fn pool_key(info: &WSAPROTOCOL_INFOW) -> (i32, i32, i32) {
    (info.iAddressFamily, info.iSocketType, info.iProtocol)
}

enum WatchStep {
    Nothing,
    Queue,
    Cancel(OpId),
    Remove,
}

impl IocpBackend {
    pub fn new() -> io::Result<Self> {
        ensure_winsock();
        let port = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
        if port.is_null() {
            return Err(win_error());
        }
        Ok(IocpBackend {
            port: Arc::new(PortHandle(port)),
            slab: OpSlab::new(empty_op),
            entries: Box::new(unsafe { zeroed() }),
            inline_completions: Vec::with_capacity(32),
            syscalls_saved_inline: 0,
            quarantine: Quarantine::new(),
            free_sockets: SocketPool::new(SOCKET_POOL_CAP),
            listener_info: FxHashMap::default(),
            untaken_accepts: FxHashSet::default(),
            watches: FxHashMap::default(),
            watch_rearm: Vec::new(),
            afd: None,
            wake_only: None,
            skip_ok: FxHashSet::default(),
            accept_socket_flags: WSA_FLAG_OVERLAPPED | crate::netsys::WSA_FLAG_NO_HANDLE_INHERIT,
            associated: FxHashSet::default(),
            foreign_completions: 0,
            dropped_completions: 0,
        })
    }

    /// May a synchronous success on this socket be handled inline (R-031)?
    fn inline_ok(&self, socket: SOCKET) -> bool {
        self.skip_ok.contains(&socket)
    }

    /// Forget everything cached under a handle VALUE. Called when the value
    /// stops naming the socket the cache describes: on detach and close,
    /// and when a fresh association proves it now names a new one.
    fn forget_value(&mut self, socket: SOCKET) {
        self.skip_ok.remove(&socket);
        self.listener_info.remove(&socket);
    }

    /// Close a socket this backend owns outright (accept and pooled sockets).
    fn close_owned(&mut self, socket: SOCKET) {
        self.associated.remove(&socket);
        self.forget_value(socket);
        unsafe { closesocket(socket) };
    }

    // ---- readiness watches (R-057) --------------------------------------

    /// The AFD helper's base handle, creating the helper on first use.
    fn ensure_afd_helper(&mut self) -> io::Result<SOCKET> {
        if let Some(a) = &self.afd {
            return Ok(a.base);
        }
        ensure_winsock();
        let mut last = io::Error::other("no address family available for the AFD helper socket");
        for af in [AF_INET, AF_INET6] {
            let s = match new_socket(af as i32, SOCK_STREAM, IPPROTO_TCP) {
                Ok(s) => s,
                Err(e) => {
                    last = e;
                    continue;
                }
            };
            let base = base_socket(s).unwrap_or(s);
            if unsafe { CreateIoCompletionPort(base as HANDLE, self.port.0, KEY_IO, 0) }.is_null() {
                last = win_error();
                unsafe { closesocket(s) };
                continue;
            }
            // Deliberately NOT skip-on-success: a poll that completes
            // synchronously still posts its packet, so a poll's slot is only
            // ever retired from a dequeued completion.
            unsafe { SetFileCompletionNotificationModes(base as HANDLE, FILE_SKIP_SET_EVENT_ON_HANDLE) };
            self.afd = Some(AfdHelper { socket: s, base });
            return Ok(base);
        }
        Err(last)
    }

    fn queue_rearm(&mut self, socket: SOCKET) {
        if let Some(w) = self.watches.get_mut(&socket) {
            if !w.queued {
                w.queued = true;
                self.watch_rearm.push(socket);
            }
        }
    }

    /// Submit the polls of every watch queued for one (start of each poll).
    fn rearm_watches(&mut self) {
        if self.watch_rearm.is_empty() {
            return;
        }
        let mut queued = std::mem::take(&mut self.watch_rearm);
        for &s in &queued {
            if let Some(w) = self.watches.get_mut(&s) {
                w.queued = false;
            }
            self.submit_poll(s);
        }
        // submit_poll never queues, so nothing was pushed meanwhile: keep
        // the allocation.
        queued.clear();
        self.watch_rearm = queued;
    }

    fn submit_poll(&mut self, socket: SOCKET) {
        let Some(w) = self.watches.get(&socket) else { return };
        if w.dead || w.poll.is_some() || !(w.read || w.write) {
            return;
        }
        let (base, read, write) = (w.base, w.read, w.write);
        let events = afd_poll_events(read, write);
        let Some(helper) = self.afd.as_ref().map(|a| a.base) else { return };
        let id = self.new_op(OpKind::Poll, socket);
        let op = &mut self.slab.get_mut(id).expect("fresh id resolves").data;
        op.afd = AfdPollInfo {
            timeout: i64::MAX,
            number_of_handles: 1,
            exclusive: 0,
            handles: [AfdPollHandleInfo { handle: base as HANDLE, events, status: 0 }],
        };
        // IO_STATUS_BLOCK overlays OVERLAPPED.Internal/InternalHigh: the same
        // aliasing ReadFile and WSARecv rely on underneath.
        op.overlapped.Internal = STATUS_PENDING as u32 as usize;
        let ov = &mut op.overlapped as *mut OVERLAPPED;
        let info = &mut op.afd as *mut AfdPollInfo;
        let status = unsafe {
            NtDeviceIoControlFile(
                helper as HANDLE,
                std::ptr::null_mut(),
                None,
                ov as *const c_void,
                ov.cast::<IO_STATUS_BLOCK>(),
                IOCTL_AFD_POLL,
                info as *const c_void,
                size_of::<AfdPollInfo>() as u32,
                info.cast(),
                size_of::<AfdPollInfo>() as u32,
            )
        };
        if nt_error(status) {
            // Refused before it was pended, so no packet is coming. Report
            // every watched side once -- the callback's own syscall then
            // surfaces the error -- and stop: re-arming a poll AFD refuses
            // would spin.
            self.retire_unreaped(id);
            if let Some(w) = self.watches.get_mut(&socket) {
                w.dead = true;
            }
            self.inline_completions.push(Completion::Ready {
                socket: socket as RawSocket,
                readable: read,
                writable: write,
            });
            return;
        }
        // Pending, or completed at once: either way its packet arrives
        // through the port (the helper is not in skip-on-success mode).
        if let Some(w) = self.watches.get_mut(&socket) {
            w.poll = Some(id);
            w.polled = events;
        }
    }

    fn cancel_poll(&mut self, id: OpId) {
        if !self.slab.mark_cancelled(id) {
            return;
        }
        let Some(helper) = self.afd.as_ref().map(|a| a.base) else { return };
        let ov = &mut self.slab.get_mut(id).expect("cancelled op is live").data.overlapped as *mut OVERLAPPED;
        // ERROR_NOT_FOUND: already completing; the packet is on its way.
        unsafe { CancelIoEx(helper as HANDLE, ov) };
    }

    fn on_poll_done(
        &mut self,
        socket: SOCKET,
        id: OpId,
        status: i32,
        events: u32,
        cancelled: bool,
        out: &mut Vec<Completion>,
    ) {
        let Some(w) = self.watches.get_mut(&socket) else { return };
        if w.poll != Some(id) {
            return;
        }
        w.poll = None;
        // A poll we cancelled answers for interest that has since changed --
        // the watch was removed, perhaps with its socket closed and the
        // number reissued to a new one watched since. Whatever it says
        // (even "closed", which would end the new watch), only re-arming
        // is right: a fresh poll reports anything still true at once.
        let outcome =
            if cancelled { PollOutcome::Cancelled } else { afd_outcome(status, events, w.read, w.write) };
        match outcome {
            PollOutcome::Ready { readable, writable } => {
                if readable || writable {
                    out.push(Completion::Ready { socket: socket as RawSocket, readable, writable });
                }
            }
            PollOutcome::Cancelled => {}
            PollOutcome::Failed => {
                w.dead = true;
                if w.read || w.write {
                    out.push(Completion::Ready {
                        socket: socket as RawSocket,
                        readable: w.read,
                        writable: w.write,
                    });
                }
            }
            // Closed under the watch: its base handle (and maybe its number)
            // no longer names it, so it is never polled again.
            PollOutcome::Closed => w.dead = true,
        }
        let step = if !(w.read || w.write) {
            WatchStep::Remove
        } else if w.dead {
            WatchStep::Nothing
        } else {
            // Level-trigger cadence: the next poll re-submits, and a
            // condition that still holds completes it at once.
            WatchStep::Queue
        };
        match step {
            WatchStep::Remove => {
                self.watches.remove(&socket);
            }
            WatchStep::Queue => self.queue_rearm(socket),
            _ => {}
        }
    }

    // ---- op slots ---------------------------------------------------------

    /// Allocate + initialize an op slot.
    fn new_op(&mut self, kind: OpKind, socket: SOCKET) -> OpId {
        let id = self.slab.post(kind);
        let op = &mut self.slab.get_mut(id).expect("fresh id resolves").data;
        op.overlapped = unsafe { zeroed() };
        op.id = id;
        op.socket = socket;
        op.accept_socket = INVALID_SOCKET;
        op.inline = false;
        id
    }

    /// Retire a slot whose op ended WITHOUT a port completion: completed
    /// now, recycled only once the port has been drained (see Quarantine).
    fn retire_unreaped(&mut self, id: OpId) {
        if let Some(slot) = self.slab.get_mut(id) {
            slot.data.inline = true;
            slot.data.dgram = Vec::new();
        }
        self.slab.complete(id);
        self.quarantine.hold(id);
    }

    /// Recycle a Completed slot the transport layer has finished with
    /// (take_accept_socket / take_recv_from_addr).
    fn retire(&mut self, id: OpId) {
        if self.slab.get(id).is_some_and(|s| s.data.inline) {
            self.quarantine.hold(id);
        } else {
            self.slab.release(id);
        }
    }

    /// Handle a post that returned synchronously under
    /// FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: surface inline (R-031: no
    /// queued completion is expected).
    fn complete_inline(&mut self, id: OpId, bytes: u32) {
        self.retire_unreaped(id);
        self.syscalls_saved_inline += 1;
        self.inline_completions.push(Completion::Io { op: id, bytes, os_error: 0 });
    }

    /// Post failed synchronously: reclaim the slot, return the error.
    fn fail_post(&mut self, id: OpId, err: io::Error) -> io::Error {
        self.retire_unreaped(id);
        err
    }

    /// The quarantine size to pass to `port_drained` after a dequeue.
    pub(crate) fn quarantine_mark(&self) -> usize {
        self.quarantine.mark()
    }

    /// After a dequeue that started at `mark` and returned `dequeued`
    /// entries: recycle every quarantined slot whose stray packet, if it
    /// had one, has now been seen.
    pub(crate) fn port_drained(&mut self, mark: usize, dequeued: usize) {
        if self.quarantine.is_empty() {
            return;
        }
        let slab = &mut self.slab;
        for id in self.quarantine.drained(mark, dequeued, POLL_BATCH) {
            slab.release(id);
        }
        // A full batch proves nothing; under sustained load every batch is
        // full, and without this the quarantine (and the slab under it)
        // grew for as long as the load lasted.
        if let Some(generation) = self.quarantine.sentinel_wanted(dequeued, POLL_BATCH) {
            let posted = unsafe {
                PostQueuedCompletionStatus(self.port.0, generation, KEY_DRAIN, std::ptr::null_mut())
            };
            if posted != 0 {
                self.quarantine.sentinel_posted(generation);
            }
        }
    }

    fn listener_protocol(&mut self, listener: SOCKET) -> io::Result<WSAPROTOCOL_INFOW> {
        if let Some(info) = self.listener_info.get(&listener) {
            return Ok(*info);
        }
        let info = protocol_info(listener)?;
        self.listener_info.insert(listener, info);
        Ok(info)
    }

    fn accept_socket_for(&mut self, listener: SOCKET) -> io::Result<SOCKET> {
        let info = self.listener_protocol(listener)?;
        // R-033: prefer a recycled socket -- of the listener's own kind.
        if let Some(s) = self.free_sockets.pop(pool_key(&info)) {
            return Ok(s);
        }
        let mut s = unsafe {
            WSASocketW(
                info.iAddressFamily,
                info.iSocketType,
                info.iProtocol,
                std::ptr::null(),
                0,
                self.accept_socket_flags,
            )
        };
        if s == INVALID_SOCKET {
            // An accepted connection is as inheritable as a listener, and
            // just as damaging in a child: it holds the peer's connection
            // open past the server's own close. WSAEINVAL means the
            // no-inherit flag was refused rather than the socket failing,
            // so drop it for good on this backend and clear the bit by
            // hand instead (the pre-Win8 path).
            let no_inherit = crate::netsys::WSA_FLAG_NO_HANDLE_INHERIT;
            if unsafe { WSAGetLastError() } != WSAEINVAL || self.accept_socket_flags & no_inherit == 0 {
                return Err(wsa_error());
            }
            self.accept_socket_flags &= !no_inherit;
            s = unsafe {
                WSASocketW(
                    info.iAddressFamily,
                    info.iSocketType,
                    info.iProtocol,
                    std::ptr::null(),
                    0,
                    self.accept_socket_flags,
                )
            };
            if s == INVALID_SOCKET {
                return Err(wsa_error());
            }
        }
        if self.accept_socket_flags & crate::netsys::WSA_FLAG_NO_HANDLE_INHERIT == 0 {
            crate::netsys::clear_handle_inherit(s);
        }
        Ok(s)
    }

    /// R-033: pool a socket DisconnectEx(TF_REUSE_SOCKET) released. It keeps
    /// its association with this port, so it stays ours for register_socket.
    fn recycle(&mut self, socket: SOCKET) {
        let key = match protocol_info(socket) {
            Ok(info) => pool_key(&info),
            Err(_) => return self.close_owned(socket),
        };
        if let Some(over) = self.free_sockets.push(key, socket) {
            return self.close_owned(over);
        }
        self.associated.insert(socket);
    }

    /// RIO-hybrid hooks: the RioBackend shares this backend's completion
    /// port (its CQ notification arrives under KEY_RIO) and drives the
    /// GetQueuedCompletionStatusEx loop itself.
    pub(crate) fn set_accept_socket_flags(&mut self, flags: u32) {
        self.accept_socket_flags = flags;
    }

    pub(crate) fn port_handle(&self) -> HANDLE {
        self.port.0
    }

    /// Pre-poll work shared with the hybrid: re-arm readiness watches and
    /// surface inline (never-queued) completions.
    pub(crate) fn pre_poll(&mut self, out: &mut Vec<Completion>) {
        self.rearm_watches();
        if !self.inline_completions.is_empty() {
            out.append(&mut self.inline_completions);
        }
    }

    /// R-031: skip completion-port posts for synchronous successes —
    /// guarded against non-IFS (LSP) providers. Idempotent.
    fn apply_skip_modes(&mut self, socket: RawSocket) {
        if self.skip_ok.contains(&socket) {
            return;
        }
        let ifs = protocol_info(socket).is_ok_and(|info| info.dwServiceFlags1 & XP1_IFS_HANDLES != 0);
        if ifs {
            let ok = unsafe {
                SetFileCompletionNotificationModes(
                    socket as HANDLE,
                    FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | FILE_SKIP_SET_EVENT_ON_HANDLE,
                )
            };
            if ok != 0 {
                self.skip_ok.insert(socket);
            }
        }
    }

    pub(crate) fn translate_entry(&mut self, entry: &OVERLAPPED_ENTRY, out: &mut Vec<Completion>) {
        if entry.lpCompletionKey == KEY_WAKEUP {
            out.push(Completion::Wakeup);
            return;
        }
        if entry.lpCompletionKey == KEY_DRAIN {
            // Every packet queued before the sentinel has now been seen,
            // in this batch or an earlier one (the port is FIFO).
            let slab = &mut self.slab;
            for id in self.quarantine.sentinel_arrived(entry.dwNumberOfBytesTransferred) {
                slab.release(id);
            }
            return;
        }
        if entry.lpOverlapped.is_null() {
            return;
        }
        // ADR-25: screen the pointer before trusting it. A completion can
        // carry an OVERLAPPED that was never ours — the kernel delivers to
        // the port the FILE OBJECT is bound to, so a handle shared across
        // processes routes another process's ops here, with a pointer valid
        // only in ITS address space. The slab's generation check cannot help:
        // it runs after the dereference that would fault. Drop and count.
        if !self.slab.contains_data_ptr(entry.lpOverlapped.cast::<u8>()) {
            self.foreign_completions += 1;
            return;
        }
        // SAFETY: the pointer is exactly a live slot's `data` (checked
        // above), OVERLAPPED is the first field of repr(C) IocpOp, and slab
        // slots are pinned until their completion is reaped (R-037).
        let op_ptr = entry.lpOverlapped.cast::<IocpOp>();
        let id = unsafe { (*op_ptr).id };
        // A packet for a slot that is not in flight belongs to an op already
        // retired without one (inline, or failed synchronously) and still
        // quarantined: acting on it would complete whatever the slot holds.
        if !matches!(self.slab.state(id), Some(OpState::Posted | OpState::Cancelled)) {
            self.dropped_completions += 1;
            return;
        }
        let Some((kind, was_cancelled)) = self.slab.complete(id) else { return };
        // R-037: the final NTSTATUS and byte count, read from the request
        // itself. The handle it was issued on may be closed (most cancelled
        // ops are reaped after closesocket) or even recycled by now.
        let (socket, status) =
            unsafe { ((*op_ptr).socket, overlapped_status((*op_ptr).overlapped.Internal)) };
        if kind == OpKind::Poll {
            let events = unsafe {
                let info = &(*op_ptr).afd;
                afd_reported_events(info.number_of_handles, info.handles[0].events)
            };
            self.slab.release(id);
            self.on_poll_done(socket, id, status, events, was_cancelled, out);
            return;
        }
        let os_error = if matches!(kind, OpKind::PipeRead | OpKind::PipeWrite) {
            pipe_error(status)
        } else {
            socket_error(status)
        };
        let bytes = if os_error == 0 { entry.dwNumberOfBytesTransferred } else { 0 };
        match kind {
            OpKind::Accept => {
                if os_error != 0 {
                    let s = unsafe { (*op_ptr).accept_socket };
                    if s != INVALID_SOCKET {
                        self.close_owned(s);
                    }
                    self.slab.release(id);
                } else {
                    // Keep the slot alive: the accepted socket + addr buffer
                    // are consumed via take_accept_socket (R-032).
                    self.untaken_accepts.insert(id);
                }
            }
            OpKind::Disconnect => {
                // R-033: recycle into the free-socket pool (cap, overflow ->
                // closesocket).
                let s = unsafe { (*op_ptr).socket };
                if os_error == 0 {
                    self.recycle(s);
                } else {
                    self.close_owned(s);
                }
                self.slab.release(id);
            }
            OpKind::Connect => {
                // A cancelled connect's socket may already be closed -- loop
                // close shuts it right after CancelIoEx -- and its handle
                // value reissued, even when the connect raced to success.
                if os_error == 0 && !was_cancelled {
                    // R-034: SO_UPDATE_CONNECT_CONTEXT on completion.
                    unsafe {
                        setsockopt(socket, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, std::ptr::null(), 0);
                    }
                }
                self.slab.release(id);
            }
            OpKind::RecvFrom => {
                // Keep the slot on success: the peer address in addr_buf is
                // consumed via take_recv_from_addr (R-058).
                if os_error != 0 {
                    self.slab.release(id);
                }
            }
            OpKind::SendTo => {
                // Free the copied-in datagram with the slot.
                unsafe { (*op_ptr).dgram = Vec::new() };
                self.slab.release(id);
            }
            _ => {
                self.slab.release(id);
            }
        }
        out.push(Completion::Io { op: id, bytes, os_error });
    }

    /// Cancel every op the kernel may still own and reap completions until
    /// none is left in flight, or `budget` runs out. Returns how many are
    /// still in flight -- their slots must then never be freed.
    ///
    /// Everything reaped goes through `translate_entry`, so it is released
    /// on exactly the evidence a tick would use; the completions themselves
    /// are discarded (teardown has nobody to deliver them to).
    pub(crate) fn quiesce(&mut self, budget: Duration) -> usize {
        let helper = self.afd.as_ref().map(|a| a.base);
        for id in self.slab.in_flight_ids() {
            let Some(slot) = self.slab.get_mut(id) else { continue };
            let handle = match slot.kind {
                OpKind::Poll => match helper {
                    Some(h) => h,
                    None => continue,
                },
                // AcceptEx runs on the listener, every other op on its own
                // socket or pipe; both are recorded in `socket`.
                _ => slot.data.socket,
            };
            let ov = &mut slot.data.overlapped as *mut OVERLAPPED;
            unsafe { CancelIoEx(handle as HANDLE, ov) };
        }
        let deadline = Instant::now() + budget;
        let mut sink: Vec<Completion> = Vec::new();
        while self.slab.in_flight() > 0 {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            let wait_ms = crate::backend::wait_millis(left).min(50) as u32;
            let mut n: u32 = 0;
            let ok = unsafe {
                GetQueuedCompletionStatusEx(
                    self.port.0,
                    self.entries.as_mut_ptr(),
                    POLL_BATCH as u32,
                    &mut n,
                    wait_ms,
                    0,
                )
            };
            if ok == 0 {
                if unsafe { GetLastError() } == WAIT_TIMEOUT {
                    continue;
                }
                break;
            }
            for i in 0..n as usize {
                let entry = self.entries[i];
                self.translate_entry(&entry, &mut sink);
            }
            sink.clear();
        }
        self.slab.in_flight()
    }
}

impl Drop for IocpBackend {
    fn drop(&mut self) {
        // The loop being dropped does not end the kernel's ownership of
        // anything still in flight: a cancelled op's OVERLAPPED is written
        // when its cancellation completes, and a readiness poll of a socket
        // that outlives the loop would complete whenever data arrived. Reap
        // all of it before the slab goes.
        let left = self.quiesce(DROP_DRAIN_BUDGET);
        // Accepted connections the transport layer never collected, and
        // sockets parked for reuse: ours alone, so close them.
        for id in self.untaken_accepts.drain() {
            if let Some(slot) = self.slab.get(id) {
                let s = slot.data.accept_socket;
                if s != INVALID_SOCKET {
                    unsafe { closesocket(s) };
                }
            }
        }
        for s in self.free_sockets.drain_all() {
            unsafe { closesocket(s) };
        }
        if left == 0 {
            if let Some(a) = self.afd.take() {
                unsafe { closesocket(a.socket) };
            }
        } else {
            // Unreapable ops would write into whatever reused their memory:
            // leak the slab, and the helper their polls were issued on.
            std::mem::forget(std::mem::replace(&mut self.slab, OpSlab::new(empty_op)));
            self.afd = None;
        }
    }
}

impl IoBackend for IocpBackend {
    fn foreign_completions(&self) -> u64 {
        self.foreign_completions + self.dropped_completions
    }

    fn register_socket(&mut self, socket: RawSocket) -> io::Result<()> {
        // Deliberately NOT short-circuited on `self.associated`. That set is
        // keyed by SOCKET VALUE, and Windows recycles those aggressively:
        // any path that closes a registered socket without calling
        // detach_socket leaves a stale entry, and the next socket handed
        // the same numeric value is then believed to be associated when it
        // is not. Its completions are never queued to our port, so the op
        // simply never completes -- an operation that hangs forever with no
        // error anywhere. Always attempt the association; an already-bound
        // file object answers ERROR_INVALID_PARAMETER, which is handled
        // below and costs one cheap syscall on the only common case
        // (attach_stream re-registering a socket tcp_connect associated).
        let handle = socket as HANDLE;
        let rc = unsafe { CreateIoCompletionPort(handle, self.port.0, KEY_IO, 0) };
        if rc.is_null() {
            let err = win_error();
            // ERROR_INVALID_PARAMETER on a VALID socket means "already
            // associated". A file object binds to exactly one IOCP for life,
            // so this cannot tell "already MINE" from "already someone
            // else's": a listener duplicated into N workers (R-090) can only
            // be associated by whichever got there first, and waving the
            // others through had them post AcceptEx whose completions were
            // delivered to the winner's port carrying an lpOverlapped valid
            // only in the loser's address space (ADR-24/25). Only a socket
            // this backend associated, and has not seen detached since, is
            // ours.
            if err.raw_os_error() == Some(87) {
                let ours = self.associated.contains(&socket);
                if trace_assoc_enabled() {
                    eprintln!(
                        "cadeloop-iocp: register_socket({socket}) ERROR_INVALID_PARAMETER; ours={ours}"
                    );
                    let _ = std::io::Write::flush(&mut std::io::stderr());
                }
                if !ours {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "socket is already bound to a completion port this loop does not own \
                         (a Windows file object binds to exactly one IOCP for life; a socket \
                         another loop or process has driven cannot be driven by this one)",
                    ));
                }
                let mut t: i32 = 0;
                let mut len = size_of::<i32>() as i32;
                let ok = unsafe {
                    getsockopt(
                        socket,
                        SOL_SOCKET,
                        0x1008, /* SO_TYPE */
                        (&mut t as *mut i32).cast(),
                        &mut len,
                    )
                };
                if ok == 0 {
                    self.apply_skip_modes(socket);
                    return Ok(());
                }
            }
            return Err(err);
        }
        // A fresh association proves a fresh file object, whatever this
        // handle VALUE named before. Anything cached under the value belongs
        // to a socket that has since been closed -- in particular `skip_ok`:
        // inheriting it made synchronous successes complete inline on a
        // socket whose skip mode was never set, while the kernel also queued
        // a packet for them, and that packet then completed whichever live
        // op had reused the slot.
        self.forget_value(socket);
        self.associated.insert(socket);
        self.apply_skip_modes(socket);
        Ok(())
    }

    /// R-051: associate a named-pipe HANDLE with the same port sockets
    /// use. No skip-modes here — every pipe completion (sync or async)
    /// still posts to the port, which keeps post_pipe_read/write simple
    /// (no inline fast path to reason about; pipes are not the hot path).
    fn register_pipe(&mut self, handle: RawSocket) -> io::Result<()> {
        // Deliberately NOT short-circuited, for exactly the reason
        // register_socket is not: a closed handle's value is reissued, and
        // a stale record made the next handle given that number look
        // associated when it was not, so its overlapped ReadFile/WriteFile
        // completion was delivered nowhere -- the future hung forever. Not
        // recorded in `associated` either: pipes are never detached, so an
        // entry would outlive its handle and vouch for whatever socket
        // reused the number.
        let rc = unsafe { CreateIoCompletionPort(handle as HANDLE, self.port.0, KEY_IO, 0) };
        if rc.is_null() {
            return Err(win_error());
        }
        self.forget_value(handle);
        Ok(())
    }

    // `buf` validity for the lifetime of the op is the same reactor-level
    // discipline post_recv/post_send rely on (the pyshim caller parks the
    // owning buffer until completion) — post_recv dodges this lint only
    // because it re-derives its pointer from a WSABUF field instead of
    // passing the parameter straight into the unsafe call; the safety
    // story is identical either way.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn post_pipe_read(&mut self, handle: RawSocket, buf: *mut u8, len: u32) -> io::Result<OpId> {
        let id = self.new_op(OpKind::PipeRead, handle);
        let ov = &mut self.slab.get_mut(id).expect("fresh id resolves").data.overlapped as *mut OVERLAPPED;
        let mut read: u32 = 0;
        let ok = unsafe { ReadFile(handle as HANDLE, buf, len, &mut read, ov) };
        if ok == 0 {
            let err = unsafe { GetLastError() };
            // ERROR_MORE_DATA: a message-mode pipe's message is longer than
            // `len`. The read DID happen -- the buffer is full -- and, its
            // status being a warning rather than an error, its packet is
            // queued like a success's. Freeing the slot here let that packet
            // complete whatever op reused it.
            if err != ERROR_IO_PENDING && err != ERROR_MORE_DATA {
                return Err(self.fail_post(id, io::Error::from_raw_os_error(err as i32)));
            }
        }
        // Synchronous success (ok != 0) still posts a completion packet
        // (no skip-modes on pipe handles) — same code path as pending.
        Ok(id)
    }

    fn post_pipe_write(&mut self, handle: RawSocket, data: &[u8]) -> io::Result<OpId> {
        let id = self.new_op(OpKind::PipeWrite, handle);
        let ov = &mut self.slab.get_mut(id).expect("fresh id resolves").data.overlapped as *mut OVERLAPPED;
        let len = data.len().min(MAX_PIPE_WRITE) as u32;
        let mut written: u32 = 0;
        let ok = unsafe { WriteFile(handle as HANDLE, data.as_ptr(), len, &mut written, ov) };
        if ok == 0 {
            let err = unsafe { GetLastError() };
            if err != ERROR_IO_PENDING {
                return Err(self.fail_post(id, io::Error::from_raw_os_error(err as i32)));
            }
        }
        Ok(id)
    }

    fn post_accept(&mut self, listener: RawSocket) -> io::Result<OpId> {
        let fns = ext_fns()?;
        let accept_socket = self.accept_socket_for(listener)?;
        let id = self.new_op(OpKind::Accept, listener);
        let op = &mut self.slab.get_mut(id).expect("fresh id resolves").data;
        op.accept_socket = accept_socket;
        let ov = &mut op.overlapped as *mut OVERLAPPED;
        let addr = op.addr_buf.as_mut_ptr();
        let mut received: u32 = 0;
        let ok = unsafe {
            (fns.accept_ex.unwrap())(
                listener,
                accept_socket,
                addr.cast(),
                0, // no receive-with-accept: complete on connect (R-032)
                ACCEPT_ADDR_LEN,
                ACCEPT_ADDR_LEN,
                &mut received,
                ov,
            )
        };
        if ok != 0 {
            // Synchronous accept (rare): with skip-modes the completion is
            // NOT queued; surface inline but keep the slot for
            // take_accept_socket. LSP sockets get it via the port instead.
            if self.inline_ok(listener) {
                if let Some(slot) = self.slab.get_mut(id) {
                    slot.data.inline = true;
                }
                self.slab.complete(id);
                self.untaken_accepts.insert(id);
                self.syscalls_saved_inline += 1;
                self.inline_completions.push(Completion::Io { op: id, bytes: 0, os_error: 0 });
            }
            return Ok(id);
        }
        let err = unsafe { WSAGetLastError() };
        if err == WSA_IO_PENDING || err as u32 == ERROR_IO_PENDING {
            return Ok(id);
        }
        self.close_owned(accept_socket);
        Err(self.fail_post(id, io::Error::from_raw_os_error(err)))
    }

    fn post_recv(&mut self, socket: RawSocket, buf: *mut u8, len: u32) -> io::Result<OpId> {
        let id = self.new_op(OpKind::Recv, socket);
        let op = &mut self.slab.get_mut(id).unwrap().data;
        op.wsabufs[0] = WSABUF { len, buf };
        let wsabuf_ptr = &mut op.wsabufs[0] as *mut WSABUF;
        let ov = &mut op.overlapped as *mut OVERLAPPED;
        let mut bytes: u32 = 0;
        let mut flags: u32 = 0;
        let rc = unsafe { WSARecv(socket, wsabuf_ptr, 1, &mut bytes, &mut flags, ov, None) };
        if rc == 0 {
            if self.inline_ok(socket) {
                // R-031: synchronous success handled inline, no queued
                // completion expected.
                self.complete_inline(id, bytes);
            }
            // Skip-modes off (LSP socket): completion arrives via the port.
            return Ok(id);
        }
        let err = unsafe { WSAGetLastError() };
        if err == WSA_IO_PENDING {
            return Ok(id);
        }
        Err(self.fail_post(id, io::Error::from_raw_os_error(err)))
    }

    fn post_recv_from(&mut self, socket: RawSocket, buf: *mut u8, len: u32) -> io::Result<OpId> {
        let id = self.new_op(OpKind::RecvFrom, socket);
        let op = &mut self.slab.get_mut(id).unwrap().data;
        op.wsabufs[0] = WSABUF { len, buf };
        op.from_len = ACCEPT_BUF_LEN as i32;
        let wsabuf_ptr = &mut op.wsabufs[0] as *mut WSABUF;
        let from_ptr = op.addr_buf.as_mut_ptr().cast();
        let from_len_ptr = &mut op.from_len as *mut i32;
        let ov = &mut op.overlapped as *mut OVERLAPPED;
        let mut bytes: u32 = 0;
        let mut flags: u32 = 0;
        let rc = unsafe {
            WSARecvFrom(socket, wsabuf_ptr, 1, &mut bytes, &mut flags, from_ptr, from_len_ptr, ov, None)
        };
        if rc == 0 {
            if self.inline_ok(socket) {
                // Inline success KEEPS the slot (addr consumed via
                // take_recv_from_addr), retired through the quarantine then.
                if let Some(slot) = self.slab.get_mut(id) {
                    slot.data.inline = true;
                }
                self.slab.complete(id);
                self.syscalls_saved_inline += 1;
                self.inline_completions.push(Completion::Io { op: id, bytes, os_error: 0 });
            }
            return Ok(id);
        }
        let err = unsafe { WSAGetLastError() };
        if err == WSA_IO_PENDING {
            return Ok(id);
        }
        Err(self.fail_post(id, io::Error::from_raw_os_error(err)))
    }

    fn take_recv_from_addr(&mut self, op: OpId) -> Option<netsys::Addr> {
        if self.slab.state(op) != Some(OpState::Completed) {
            return None;
        }
        let addr = self.slab.get(op).and_then(|s| {
            let n = s.data.from_len.clamp(0, ACCEPT_BUF_LEN as i32) as usize;
            netsys::parse_any_sockaddr(&s.data.addr_buf[..n])
        });
        self.retire(op);
        addr
    }

    fn post_send_to(
        &mut self,
        socket: RawSocket,
        data: &[u8],
        addr: Option<&std::net::SocketAddr>,
    ) -> io::Result<OpId> {
        // A datagram goes out whole or not at all, and a WSABUF length is a
        // u32: `as u32` would hand the kernel a shorter, valid-looking
        // datagram instead of the WSAEMSGSIZE the OS gives for any size it
        // cannot send.
        if u32::try_from(data.len()).is_err() {
            return Err(io::Error::from_raw_os_error(WSAEMSGSIZE));
        }
        let id = self.new_op(OpKind::SendTo, socket);
        let op = &mut self.slab.get_mut(id).unwrap().data;
        op.dgram = data.to_vec();
        let (to_ptr, to_len): (*const _, i32) = match addr {
            Some(a) => {
                let sa = netsys::build_sockaddr(*a);
                op.addr_buf[..sa.len].copy_from_slice(&sa.buf[..sa.len]);
                (op.addr_buf.as_ptr().cast(), sa.len as i32)
            }
            None => (std::ptr::null(), 0), // connected-mode send()
        };
        op.wsabufs[0] = WSABUF { len: op.dgram.len() as u32, buf: op.dgram.as_mut_ptr() };
        let wsabuf_ptr = &mut op.wsabufs[0] as *mut WSABUF;
        let ov = &mut op.overlapped as *mut OVERLAPPED;
        let mut bytes: u32 = 0;
        let rc = unsafe { WSASendTo(socket, wsabuf_ptr, 1, &mut bytes, 0, to_ptr, to_len, ov, None) };
        if rc == 0 {
            if self.inline_ok(socket) {
                self.complete_inline(id, bytes);
            }
            return Ok(id);
        }
        let err = unsafe { WSAGetLastError() };
        if err == WSA_IO_PENDING {
            return Ok(id);
        }
        Err(self.fail_post(id, io::Error::from_raw_os_error(err)))
    }

    fn post_send(&mut self, socket: RawSocket, bufs: &[IoSlice]) -> io::Result<OpId> {
        debug_assert!(!bufs.is_empty() && bufs.len() <= MAX_GATHER, "R-035: 1..=16 WSABUFs");
        let id = self.new_op(OpKind::Send, socket);
        let op = &mut self.slab.get_mut(id).unwrap().data;
        // The completion's byte count is a DWORD: a gather past 4 GiB would
        // wrap it, and the transport would then advance its queue by the
        // wrong amount. Trailing slices that would overflow it wait for the
        // next post (partial sends resume either way).
        let mut n = 0usize;
        let mut total: u64 = 0;
        for b in bufs.iter().take(MAX_GATHER) {
            if n > 0 && total + b.len as u64 > u32::MAX as u64 {
                break;
            }
            op.wsabufs[n] = WSABUF { len: b.len, buf: b.ptr as *mut u8 };
            total += b.len as u64;
            n += 1;
        }
        let wsabuf_ptr = op.wsabufs.as_mut_ptr();
        let ov = &mut op.overlapped as *mut OVERLAPPED;
        let mut bytes: u32 = 0;
        let rc = unsafe { WSASend(socket, wsabuf_ptr, n as u32, &mut bytes, 0, ov, None) };
        if rc == 0 {
            if self.inline_ok(socket) {
                self.complete_inline(id, bytes);
            }
            return Ok(id);
        }
        let err = unsafe { WSAGetLastError() };
        if err == WSA_IO_PENDING {
            return Ok(id);
        }
        Err(self.fail_post(id, io::Error::from_raw_os_error(err)))
    }

    fn post_connect(&mut self, socket: RawSocket, addr: &[u8]) -> io::Result<OpId> {
        let fns = ext_fns()?;
        if addr.len() < 2 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "sockaddr too short"));
        }
        // Read the family from the bytes: `addr` carries no alignment
        // guarantee, so a `*const SOCKADDR` field read would be misaligned.
        let family = u16::from_ne_bytes([addr[0], addr[1]]);
        // R-034: ConnectEx requires a bound socket; bind to wildcard :0.
        let rc = unsafe {
            if family == AF_INET {
                let mut local: SOCKADDR_IN = zeroed();
                local.sin_family = AF_INET;
                bind(socket, (&local as *const SOCKADDR_IN).cast(), size_of::<SOCKADDR_IN>() as i32)
            } else if family == AF_INET6 {
                let mut local: SOCKADDR_IN6 = zeroed();
                local.sin6_family = AF_INET6;
                bind(socket, (&local as *const SOCKADDR_IN6).cast(), size_of::<SOCKADDR_IN6>() as i32)
            } else {
                0
            }
        };
        if rc == SOCKET_ERROR {
            let err = unsafe { WSAGetLastError() };
            // WSAEINVAL: already bound (an explicit local_addr), which is
            // what ConnectEx needs. Anything else -- WSAENOBUFS from an
            // exhausted ephemeral range, most often -- is the real reason
            // the connect cannot happen, and ignoring it surfaced as
            // ConnectEx's WSAEINVAL ("not bound") instead.
            if err != WSAEINVAL {
                return Err(io::Error::from_raw_os_error(err));
            }
        }
        let id = self.new_op(OpKind::Connect, socket);
        let ov = &mut self.slab.get_mut(id).expect("fresh id resolves").data.overlapped as *mut OVERLAPPED;
        let mut sent: u32 = 0;
        let ok = unsafe {
            (fns.connect_ex.unwrap())(
                socket,
                addr.as_ptr().cast::<SOCKADDR>(),
                addr.len() as i32,
                std::ptr::null(),
                0,
                &mut sent,
                ov,
            )
        };
        if ok != 0 {
            unsafe {
                setsockopt(socket, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, std::ptr::null(), 0);
            }
            if self.inline_ok(socket) {
                self.complete_inline(id, 0);
            }
            return Ok(id);
        }
        let err = unsafe { WSAGetLastError() };
        if err == WSA_IO_PENDING || err as u32 == ERROR_IO_PENDING {
            return Ok(id);
        }
        Err(self.fail_post(id, io::Error::from_raw_os_error(err)))
    }

    fn post_disconnect_reuse(&mut self, socket: RawSocket) -> io::Result<OpId> {
        let fns = ext_fns()?;
        let id = self.new_op(OpKind::Disconnect, socket);
        let ov = &mut self.slab.get_mut(id).expect("fresh id resolves").data.overlapped as *mut OVERLAPPED;
        let ok = unsafe { (fns.disconnect_ex.unwrap())(socket, ov, TF_REUSE_SOCKET, 0) };
        if ok != 0 {
            if self.inline_ok(socket) {
                // Synchronous, and no packet will follow: recycle now (R-033).
                self.retire_unreaped(id);
                self.syscalls_saved_inline += 1;
                self.recycle(socket);
                self.inline_completions.push(Completion::Io { op: id, bytes: 0, os_error: 0 });
            }
            // Otherwise its packet arrives like any other completion's. This
            // used to fall through to WSAGetLastError(), whose stale value
            // could fail the post while that packet was already queued.
            return Ok(id);
        }
        let err = unsafe { WSAGetLastError() };
        if err == WSA_IO_PENDING || err as u32 == ERROR_IO_PENDING {
            return Ok(id);
        }
        Err(self.fail_post(id, io::Error::from_raw_os_error(err)))
    }

    /// R-057 readiness: one IOCTL_AFD_POLL per watched socket, for the
    /// union of the wanted directions, re-submitted after each completion
    /// (level-triggered, like select()). A zero-byte recv probe consumed
    /// whole datagrams, a zero-byte send probe transmitted empty ones, and
    /// both failed instantly on listening and connecting sockets -- which
    /// re-armed every tick, a core spun at 100% for as long as a
    /// sock_accept waited.
    fn set_watch(&mut self, socket: RawSocket, readable: bool, writable: bool) -> io::Result<()> {
        let s = socket as SOCKET;
        if !(readable || writable) {
            let step = match self.watches.get_mut(&s) {
                None => WatchStep::Nothing,
                Some(w) => {
                    w.read = false;
                    w.write = false;
                    match w.poll {
                        // The entry goes when the cancelled poll is reaped.
                        Some(id) => WatchStep::Cancel(id),
                        None => WatchStep::Remove,
                    }
                }
            };
            match step {
                WatchStep::Cancel(id) => self.cancel_poll(id),
                WatchStep::Remove => {
                    self.watches.remove(&s);
                }
                _ => {}
            }
            return Ok(());
        }
        self.ensure_afd_helper()?;
        let restart = self.watches.get(&s).is_none_or(|w| w.dead || !(w.read || w.write));
        if restart {
            // A watch (re)starting learns its base handle afresh: the number
            // may name a different socket than when it was last watched.
            let base = base_socket(s)?;
            let w = self.watches.entry(s).or_insert(Watch {
                read: false,
                write: false,
                base,
                poll: None,
                polled: 0,
                queued: false,
                dead: false,
            });
            w.base = base;
            w.dead = false;
        }
        let w = self.watches.get_mut(&s).expect("present: inserted above or already watched");
        w.read = readable;
        w.write = writable;
        let want = afd_poll_events(readable, writable);
        let step = match w.poll {
            // The poll in flight already asks for every wanted event; what
            // it reports is filtered against the watch when it completes.
            Some(_) if w.polled & want == want => WatchStep::Nothing,
            // Its completion (CANCELLED) re-arms with the new set.
            Some(id) => WatchStep::Cancel(id),
            None => WatchStep::Queue,
        };
        match step {
            WatchStep::Cancel(id) => self.cancel_poll(id),
            WatchStep::Queue => self.queue_rearm(s),
            _ => {}
        }
        Ok(())
    }

    fn detach_socket(&mut self, socket: RawSocket) {
        self.associated.remove(&socket);
        // fd-number reuse must not inherit skip modes or protocol info.
        self.forget_value(socket);
        // A watch on a socket about to be closed: stop polling it.
        let _ = self.set_watch(socket, false, false);
        // In-flight ops on the socket deliver ABORTED completions via the
        // port after closesocket; the slab reaps them there (R-037).
    }

    fn cancel(&mut self, op: OpId) -> io::Result<()> {
        if self.slab.get(op).is_some_and(|s| s.kind == OpKind::Poll) {
            self.cancel_poll(op);
            return Ok(());
        }
        // R-037: {Posted -> Cancelled} exactly once; CancelIoEx thereafter.
        if !self.slab.mark_cancelled(op) {
            return Ok(());
        }
        let (socket, ov) = {
            let slot = self.slab.get_mut(op).expect("cancelled op is live");
            (slot.data.socket, &mut slot.data.overlapped as *mut OVERLAPPED)
        };
        let ok = unsafe { CancelIoEx(socket as HANDLE, ov) };
        if ok == 0 {
            let err = unsafe { GetLastError() };
            // ERROR_NOT_FOUND: already completed — the completion is still
            // in (or headed to) the port; the slot is reaped there (R-037).
            if err != ERROR_NOT_FOUND {
                return Err(io::Error::from_raw_os_error(err as i32));
            }
        }
        Ok(())
    }

    fn take_accept_socket(&mut self, op: OpId) -> io::Result<RawSocket> {
        if self.slab.state(op) != Some(OpState::Completed) || !self.untaken_accepts.remove(&op) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "accept op not completed"));
        }
        let (accepted, listener) = {
            let slot = self.slab.get(op).expect("completed op is live");
            (slot.data.accept_socket, slot.data.socket)
        };
        // R-032: inherit listener properties.
        let rc = unsafe {
            setsockopt(
                accepted,
                SOL_SOCKET,
                SO_UPDATE_ACCEPT_CONTEXT,
                (&listener as *const SOCKET).cast(),
                size_of::<SOCKET>() as i32,
            )
        };
        self.retire(op);
        if rc == SOCKET_ERROR {
            let err = wsa_error();
            self.close_owned(accepted);
            return Err(err);
        }
        Ok(accepted)
    }

    fn has_io_interest(&self) -> bool {
        // Staged completions always poll, and so does ANY pending watch
        // re-arm -- even the signal-wakeup fd's, or its fired poll would
        // never repost and a later park could sleep unwakeable. Beyond
        // that: in-flight ops other than the wake fd's own poll. O(1): a
        // watch is always either polling (in flight), queued, or dead.
        if !self.inline_completions.is_empty() || !self.watch_rearm.is_empty() {
            return true;
        }
        let wake_polls = self
            .wake_only
            .and_then(|w| self.watches.get(&(w as SOCKET)))
            .map_or(0, |w| w.poll.is_some() as usize);
        self.slab.in_flight() > wake_polls
    }

    fn set_wake_only(&mut self, fd: Option<RawSocket>) {
        self.wake_only = fd;
    }

    fn take_staged(&mut self, out: &mut Vec<Completion>) -> usize {
        // R-031 inline completions were never queued to the port: handing
        // them over needs no GetQueuedCompletionStatusEx.
        let n = self.inline_completions.len();
        if n > 0 {
            out.append(&mut self.inline_completions);
        }
        n
    }

    fn poll(&mut self, out: &mut Vec<Completion>, timeout: Option<Duration>) -> io::Result<usize> {
        let before = out.len();
        self.rearm_watches();
        // R-031: inline completions first — they were never queued.
        if !self.inline_completions.is_empty() {
            out.append(&mut self.inline_completions);
        }
        let timeout_ms: u32 = match timeout {
            _ if out.len() > before => 0, // work already available
            Some(t) => crate::backend::wait_millis(t).min(u32::MAX as u128 - 1) as u32,
            None => 0,
        };
        let mark = self.quarantine.mark();
        let mut n: u32 = 0;
        let ok = unsafe {
            GetQueuedCompletionStatusEx(
                self.port.0,
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
                self.port_drained(mark, 0);
                return Ok(out.len() - before);
            }
            return Err(io::Error::from_raw_os_error(err as i32));
        }
        for i in 0..n as usize {
            let entry = self.entries[i];
            self.translate_entry(&entry, out);
        }
        self.port_drained(mark, n as usize);
        Ok(out.len() - before)
    }

    fn wakeup_handle(&self) -> Arc<dyn Wakeup> {
        Arc::new(IocpWakeup { port: self.port.clone(), posts: AtomicU64::new(0) })
    }

    fn name(&self) -> &'static str {
        "iocp"
    }
}

/// ADR-24 diagnostic gate (CADELOOP_TRACE_ASSOC): reports when a socket is
/// already bound to a completion port we did not bind it to. Cached once
/// per process — register_socket runs per connection.
fn trace_assoc_enabled() -> bool {
    static T: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *T.get_or_init(|| std::env::var_os("CADELOOP_TRACE_ASSOC").is_some())
}

/// R-038: per-connection socket options. SND/RCVBUF stay at OS defaults
/// unless config overrides (caller applies those separately).
pub fn prepare_conn_socket(socket: RawSocket, loopback_fast_path: bool) -> io::Result<()> {
    let on: u32 = 1;
    let rc = unsafe {
        setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, (&on as *const u32).cast(), size_of::<u32>() as i32)
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_error());
    }
    if loopback_fast_path {
        // Attempted; failure ignored per R-038 (benchmark-only relevance,
        // documented with the SIO_LOOPBACK_FAST_PATH disclosure of R-131).
        let mut enabled: u32 = 1;
        let mut bytes: u32 = 0;
        unsafe {
            WSAIoctl(
                socket,
                SIO_LOOPBACK_FAST_PATH,
                (&mut enabled as *mut u32).cast(),
                size_of::<u32>() as u32,
                std::ptr::null_mut(),
                0,
                &mut bytes,
                std::ptr::null_mut(),
                None,
            );
        }
    }
    Ok(())
}

/// R-038: listener options (TCP Fast Open when cfg.tfo).
pub fn prepare_listener(socket: RawSocket, tfo: bool) -> io::Result<()> {
    if tfo {
        let on: u32 = 1;
        let rc = unsafe {
            setsockopt(socket, IPPROTO_TCP, TCP_FASTOPEN, (&on as *const u32).cast(), size_of::<u32>() as i32)
        };
        if rc == SOCKET_ERROR {
            return Err(wsa_error());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Windows-only (this module compiles only there): they drive the real
    //! port, AFD and Winsock, and run in Windows CI.

    use super::*;
    use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
    use std::os::windows::io::{AsRawSocket, IntoRawSocket};
    use windows_sys::Win32::Networking::WinSock::{connect, getpeername, ioctlsocket, FIONBIO, SO_ERROR};

    fn raw<T: AsRawSocket>(s: &T) -> RawSocket {
        s.as_raw_socket() as RawSocket
    }

    /// Poll until `want` matches a completion or `limit` passes; returns
    /// everything seen.
    fn poll_until(
        b: &mut IocpBackend,
        limit: Duration,
        mut want: impl FnMut(&Completion) -> bool,
    ) -> Vec<Completion> {
        let deadline = Instant::now() + limit;
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            let mut out = Vec::new();
            b.poll(&mut out, Some(Duration::from_millis(20))).unwrap();
            let hit = out.iter().any(&mut want);
            seen.extend(out);
            if hit {
                break;
            }
        }
        seen
    }

    /// Poll until nothing is in flight or `limit` passes.
    fn reap_all(b: &mut IocpBackend, limit: Duration) {
        let deadline = Instant::now() + limit;
        while b.slab.in_flight() > 0 && Instant::now() < deadline {
            b.poll(&mut Vec::new(), Some(Duration::from_millis(20))).unwrap();
        }
    }

    fn ready(socket: RawSocket, readable: bool, writable: bool) -> impl FnMut(&Completion) -> bool {
        move |c| {
            matches!(c, Completion::Ready { socket: s, readable: r, writable: w }
                if *s == socket && (!readable || *r) && (!writable || *w))
        }
    }

    fn so_error(s: SOCKET) -> i32 {
        let mut e: i32 = 0;
        let mut len = size_of::<i32>() as i32;
        unsafe { getsockopt(s, SOL_SOCKET, SO_ERROR, (&mut e as *mut i32).cast(), &mut len) };
        e
    }

    fn connected(s: SOCKET) -> bool {
        let mut sa: SOCKADDR_STORAGE = unsafe { zeroed() };
        let mut len = size_of::<SOCKADDR_STORAGE>() as i32;
        unsafe { getpeername(s, (&mut sa as *mut SOCKADDR_STORAGE).cast(), &mut len) == 0 }
    }

    fn blocking_connect(s: SOCKET, to: SocketAddr) -> i32 {
        let sa = netsys::build_sockaddr(to);
        unsafe { connect(s, sa.buf.as_ptr().cast(), sa.len as i32) }
    }

    #[test]
    fn local_ntstatus_constants_match_the_headers() {
        use super::super::iocp_util as u;
        use windows_sys::Win32::Foundation as f;
        assert_eq!(u::STATUS_PENDING, f::STATUS_PENDING);
        assert_eq!(u::STATUS_BUFFER_OVERFLOW, f::STATUS_BUFFER_OVERFLOW);
        assert_eq!(u::STATUS_CANCELLED, f::STATUS_CANCELLED);
        assert_eq!(u::STATUS_PIPE_BROKEN, f::STATUS_PIPE_BROKEN);
        assert_eq!(socket_error(f::STATUS_CANCELLED), 995, "is_cancelled_error's code");
        assert_eq!(socket_error(f::STATUS_CONNECTION_RESET), 10054);
        assert_eq!(socket_error(f::STATUS_CONNECTION_REFUSED), 10061);
        assert_eq!(socket_error(f::STATUS_SUCCESS), 0);
        // Pipes report what GetOverlappedResult would: RtlNtStatusToDosError.
        assert_eq!(pipe_error(f::STATUS_PIPE_BROKEN), 109, "ERROR_BROKEN_PIPE");
        assert_eq!(pipe_error(f::STATUS_CANCELLED), 995);
        // A message longer than the read is a full, valid read (W4).
        assert_eq!(pipe_error(f::STATUS_BUFFER_OVERFLOW), 0);
        // AFD_POLL_INFO as afd.sys lays it out on x64.
        if cfg!(target_pointer_width = "64") {
            assert_eq!(size_of::<AfdPollHandleInfo>(), 16);
            assert_eq!(size_of::<AfdPollInfo>(), 32);
        }
    }

    /// W1: a registered socket closed WITHOUT detach leaves its value in
    /// `skip_ok`. The next socket handed that value used to inherit it --
    /// synchronous successes were completed inline while the kernel, never
    /// told to skip, also queued a packet for them.
    #[test]
    fn a_recycled_value_never_inherits_skip_mode() {
        let mut b = IocpBackend::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let a = netsys::create_tcp(netsys::AF_INET).unwrap();
        b.register_socket(a).unwrap();
        assert!(b.skip_ok.contains(&a), "an IFS socket runs in skip mode");
        netsys::close(a); // no detach: the stale entry the bug fed on
        let s = netsys::create_tcp(netsys::AF_INET).unwrap();
        if s != a {
            netsys::close(s);
            eprintln!("handle value not recycled ({a} -> {s}); nothing to prove here");
            return;
        }
        assert_eq!(blocking_connect(s, addr), 0);
        let (_peer, _) = listener.accept().unwrap();
        b.register_socket(s).unwrap();
        let payload = *b"hello";
        let mut ops = Vec::new();
        for _ in 0..10 {
            let saved = b.syscalls_saved_inline;
            ops.push(
                b.post_send(s, &[IoSlice { ptr: payload.as_ptr(), len: payload.len() as u32 }]).unwrap(),
            );
            if b.syscalls_saved_inline > saved {
                break; // completed inline: the case that matters
            }
        }
        let mut seen = Vec::new();
        for _ in 0..5 {
            b.poll(&mut seen, Some(Duration::from_millis(20))).unwrap();
        }
        for op in &ops {
            let n = seen.iter().filter(|c| matches!(c, Completion::Io { op: o, .. } if o == op)).count();
            assert_eq!(n, 1, "{op} completed {n} times");
        }
        assert_eq!(b.dropped_completions, 0, "a packet arrived for an inline completion");
        b.detach_socket(s);
        netsys::close(s);
    }

    /// W13: "already associated" is only ours while we have not seen the
    /// socket detached; a value we once associated, reused by a socket
    /// another port owns, must be refused.
    #[test]
    fn a_socket_bound_to_another_port_is_refused() {
        let mut mine = IocpBackend::new().unwrap();
        let mut theirs = IocpBackend::new().unwrap();
        let a = netsys::create_tcp(netsys::AF_INET).unwrap();
        mine.register_socket(a).unwrap();
        mine.register_socket(a).expect("re-registering our own socket is fine");
        mine.detach_socket(a);
        netsys::close(a);
        let s = netsys::create_tcp(netsys::AF_INET).unwrap();
        theirs.register_socket(s).unwrap();
        let err = mine.register_socket(s).expect_err("bound to another port");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        theirs.detach_socket(s);
        netsys::close(s);
    }

    /// W2: the zero-byte recv probe consumed the datagram it reported.
    #[test]
    fn a_udp_read_watch_leaves_the_datagram_queued() {
        let mut b = IocpBackend::new().unwrap();
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let tx = UdpSocket::bind("127.0.0.1:0").unwrap();
        rx.set_nonblocking(true).unwrap();
        b.set_watch(raw(&rx), true, false).unwrap();
        tx.send_to(b"ping", rx.local_addr().unwrap()).unwrap();
        let seen = poll_until(&mut b, Duration::from_secs(5), ready(raw(&rx), true, false));
        assert!(seen.iter().any(ready(raw(&rx), true, false)), "no readiness: {seen:?}");
        let mut buf = [0u8; 16];
        let (n, _) = rx.recv_from(&mut buf).expect("the watch must not consume the datagram");
        assert_eq!(&buf[..n], b"ping");
        b.set_watch(raw(&rx), false, false).unwrap();
    }

    /// W2: the zero-byte send probe transmitted an empty datagram to a
    /// connected peer every time it was armed.
    #[test]
    fn a_udp_write_watch_sends_nothing() {
        let mut b = IocpBackend::new().unwrap();
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let tx = UdpSocket::bind("127.0.0.1:0").unwrap();
        tx.connect(rx.local_addr().unwrap()).unwrap();
        b.set_watch(raw(&tx), false, true).unwrap();
        let seen = poll_until(&mut b, Duration::from_secs(5), ready(raw(&tx), false, true));
        assert!(seen.iter().any(ready(raw(&tx), false, true)), "no writability: {seen:?}");
        for _ in 0..3 {
            b.poll(&mut Vec::new(), Some(Duration::from_millis(10))).unwrap();
        }
        b.set_watch(raw(&tx), false, false).unwrap();
        rx.set_nonblocking(true).unwrap();
        let mut buf = [0u8; 16];
        let err = rx.recv_from(&mut buf).expect_err("an empty datagram reached the peer");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    }

    /// W5: a read probe on a listening socket failed at once (WSAENOTCONN)
    /// and re-armed every poll, so sock_accept spun a core while it waited.
    #[test]
    fn an_idle_listener_watch_is_quiet_until_a_client_connects() {
        let mut b = IocpBackend::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let ls = raw(&listener);
        b.set_watch(ls, true, false).unwrap();
        let mut idle = Vec::new();
        for _ in 0..5 {
            b.poll(&mut idle, Some(Duration::from_millis(20))).unwrap();
        }
        assert!(idle.is_empty(), "an idle listener produced {idle:?}");
        let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let seen = poll_until(&mut b, Duration::from_secs(5), ready(ls, true, false));
        assert!(seen.iter().any(ready(ls, true, false)), "no accept readiness: {seen:?}");
        listener.accept().expect("readable means a connection is waiting");
        b.set_watch(ls, false, false).unwrap();
    }

    /// W5: a write probe on a connecting socket failed at once
    /// (WSAENOTCONN), so the watch fired, uselessly, every poll until the
    /// handshake resolved. The first writability must now mean something.
    #[test]
    fn a_connecting_socket_is_writable_only_once_the_connect_resolves() {
        let mut b = IocpBackend::new().unwrap();
        // Nothing listens here: a loopback connect to a closed port takes
        // Windows about two seconds of SYN retries to refuse.
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        let s = netsys::create_tcp(netsys::AF_INET).unwrap();
        let mut on: u32 = 1;
        assert_eq!(unsafe { ioctlsocket(s, FIONBIO, &mut on) }, 0);
        let rc = blocking_connect(s, port);
        assert!(rc != 0 && unsafe { WSAGetLastError() } == 10035, "expected WSAEWOULDBLOCK");
        b.set_watch(s, false, true).unwrap();
        let seen = poll_until(&mut b, Duration::from_secs(10), ready(s, false, true));
        assert!(seen.iter().any(ready(s, false, true)), "connect never resolved: {seen:?}");
        assert!(so_error(s) != 0 || connected(s), "writable before the connect resolved");
        b.set_watch(s, false, false).unwrap();
        reap_all(&mut b, Duration::from_secs(5));
        netsys::close(s);
    }

    /// W3: removing a watch left its probe posted for good -- and a slab
    /// freed under it once the loop went away.
    #[test]
    fn removing_a_watch_cancels_and_reaps_its_poll() {
        let mut b = IocpBackend::new().unwrap();
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        b.set_watch(raw(&rx), true, false).unwrap();
        b.poll(&mut Vec::new(), Some(Duration::ZERO)).unwrap();
        assert_eq!(b.slab.in_flight(), 1, "one poll in flight");
        b.set_watch(raw(&rx), false, false).unwrap();
        reap_all(&mut b, Duration::from_secs(5));
        assert_eq!(b.slab.in_flight(), 0, "the cancelled poll was reaped");
        assert!(b.watches.is_empty());
    }

    /// A socket closed BEFORE its watch is removed: AFD has already
    /// completed the poll with LOCAL_CLOSE when the cancel arrives. If the
    /// number is then reissued and watched again before that completion is
    /// reaped, the stale "closed" must not end the new watch.
    #[test]
    fn a_stale_close_does_not_end_a_watch_on_a_reissued_number() {
        let mut b = IocpBackend::new().unwrap();
        let a = UdpSocket::bind("127.0.0.1:0").unwrap().into_raw_socket() as RawSocket;
        b.set_watch(a, true, false).unwrap();
        b.poll(&mut Vec::new(), Some(Duration::ZERO)).unwrap();
        netsys::close(a);
        std::thread::sleep(Duration::from_millis(50)); // let AFD complete the poll
        b.set_watch(a, false, false).unwrap();
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        if raw(&rx) != a {
            eprintln!("handle value not reissued ({a} -> {}); checking the plain case", raw(&rx));
        }
        b.set_watch(raw(&rx), true, false).unwrap();
        let tx = UdpSocket::bind("127.0.0.1:0").unwrap();
        tx.send_to(b"ping", rx.local_addr().unwrap()).unwrap();
        let seen = poll_until(&mut b, Duration::from_secs(5), ready(raw(&rx), true, false));
        assert!(seen.iter().any(ready(raw(&rx), true, false)), "the new watch never fired: {seen:?}");
        b.set_watch(raw(&rx), false, false).unwrap();
    }

    /// W3: teardown cancels and reaps what the kernel still owns instead of
    /// freeing the slab under it.
    #[test]
    fn quiesce_reaps_in_flight_ops() {
        let mut b = IocpBackend::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        let ss = raw(&server);
        b.register_socket(ss).unwrap();
        let mut buf = [0u8; 64];
        b.post_recv(ss, buf.as_mut_ptr(), buf.len() as u32).unwrap();
        b.set_watch(raw(&client), true, false).unwrap();
        b.poll(&mut Vec::new(), Some(Duration::ZERO)).unwrap();
        assert_eq!(b.slab.in_flight(), 2, "a pending recv and a pending poll");
        assert_eq!(b.quiesce(Duration::from_secs(2)), 0);
        b.detach_socket(ss);
    }

    /// C15: the signal-wakeup fd's own poll is not I/O interest, and the
    /// answer costs no scan.
    #[test]
    fn the_wake_fds_poll_alone_is_not_io_interest() {
        let mut b = IocpBackend::new().unwrap();
        let wake = UdpSocket::bind("127.0.0.1:0").unwrap();
        b.set_watch(raw(&wake), true, false).unwrap();
        b.set_wake_only(Some(raw(&wake)));
        b.poll(&mut Vec::new(), Some(Duration::ZERO)).unwrap();
        assert!(!b.has_io_interest());
        let other = UdpSocket::bind("127.0.0.1:0").unwrap();
        b.set_watch(raw(&other), true, false).unwrap();
        assert!(b.has_io_interest(), "a queued re-arm is interest");
        b.poll(&mut Vec::new(), Some(Duration::ZERO)).unwrap();
        assert!(b.has_io_interest(), "another socket's poll is interest");
    }

    /// W11: a bind failure is the real error; ignoring it surfaced as
    /// ConnectEx's WSAEINVAL ("socket not bound").
    #[test]
    fn a_connect_whose_bind_fails_reports_the_bind_error() {
        let mut b = IocpBackend::new().unwrap();
        let s = netsys::create_tcp(netsys::AF_INET6).unwrap();
        b.register_socket(s).unwrap();
        let v4 = netsys::build_sockaddr("127.0.0.1:9".parse().unwrap());
        let err = b.post_connect(s, &v4.buf[..v4.len]).expect_err("an IPv4 bind on an IPv6 socket");
        assert_ne!(err.raw_os_error(), Some(WSAEINVAL), "the bind error was swallowed");
        b.detach_socket(s);
        netsys::close(s);
    }

    /// W12: one free-socket pool for every listener handed an IPv6 listener
    /// a recycled IPv4 socket, and the AcceptEx failed.
    #[test]
    fn recycled_sockets_only_serve_listeners_of_their_kind() {
        let v6 = match TcpListener::bind("[::1]:0") {
            Ok(l) => l,
            Err(e) => {
                eprintln!("no IPv6 loopback ({e}); skipping");
                return;
            }
        };
        let mut b = IocpBackend::new().unwrap();
        let v4 = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(v4.local_addr().unwrap()).unwrap();
        let (mut server, _) = v4.accept().unwrap();
        drop(client);
        // Take the peer's FIN first: closing second, our side skips the
        // TIME_WAIT that DisconnectEx(TF_REUSE_SOCKET) would otherwise wait out.
        assert_eq!(std::io::Read::read(&mut server, &mut [0u8; 1]).unwrap(), 0);
        let ss = server.into_raw_socket() as RawSocket;
        b.register_socket(ss).unwrap();
        let op = b.post_disconnect_reuse(ss).unwrap();
        let seen = poll_until(
            &mut b,
            Duration::from_secs(10),
            |c| matches!(c, Completion::Io { op: o, .. } if *o == op),
        );
        let done = seen.iter().find_map(|c| match c {
            Completion::Io { op: o, os_error, .. } if *o == op => Some(*os_error),
            _ => None,
        });
        assert_eq!(done, Some(0), "DisconnectEx did not complete: {seen:?}");
        assert_eq!(b.free_sockets.len(), 1, "the socket was pooled");
        let l6 = raw(&v6);
        b.register_socket(l6).unwrap();
        let accept = b.post_accept(l6).expect("an IPv6 accept must not be handed the IPv4 socket");
        assert_eq!(b.free_sockets.len(), 1, "the IPv4 socket is still waiting for an IPv4 listener");
        b.cancel(accept).unwrap();
        b.detach_socket(l6);
        drop(v6);
        assert_eq!(b.quiesce(Duration::from_secs(2)), 0);
    }
}
