//! IOCP backend (R-030..R-038).
//!
//! Techniques implemented per spec:
//!
//! * R-030: `GetQueuedCompletionStatusEx`, batch 256, one call per tick.
//! * R-031: `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS | FILE_SKIP_SET_EVENT_ON_HANDLE`
//!   on every socket; synchronous returns are surfaced as inline
//!   completions (never ALSO reaped from the port). Non-IFS providers
//!   (LSPs) detected via `SO_PROTOCOL_INFOW` / `XP1_IFS_HANDLES`, in which
//!   case the skip modes are not applied for that socket.
//! * R-032: `AcceptEx` + `SO_UPDATE_ACCEPT_CONTEXT`; the accept POOL
//!   (pre-posted, replenish-on-completion) lives in the M1 listener
//!   transport — this layer provides the primitive.
//! * R-033: `DisconnectEx(TF_REUSE_SOCKET)` + free-socket pool (cap 4096)
//!   consumed by subsequent accept posts.
//! * R-034: `ConnectEx` with bind-before-connect + `SO_UPDATE_CONNECT_CONTEXT`.
//! * R-035: scatter/gather `WSASend`, up to 16 `WSABUF`s.
//! * R-037: per-op `OVERLAPPED` pinned in the [`OpSlab`]; `CancelIoEx`
//!   tolerating `ERROR_NOT_FOUND`; slots recycle only after their
//!   completion is reaped.
//! * R-038: `TCP_NODELAY`, optional `SIO_LOOPBACK_FAST_PATH`, optional
//!   `TCP_FASTOPEN` via [`prepare_conn_socket`] / [`prepare_listener`].
//!
//! `TransmitFile` (R-036) and the UDP paths (R-058) land with their
//! transports (M1/M4).

#![allow(clippy::missing_safety_doc)]

use std::collections::HashMap;
use std::io;
use std::mem::{size_of, zeroed};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;

use windows_sys::core::GUID;
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, ERROR_IO_PENDING, ERROR_NOT_FOUND, HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT,
};
use windows_sys::Win32::Networking::WinSock::{
    bind, closesocket, getsockopt, setsockopt, WSAGetLastError, WSAGetOverlappedResult, WSAIoctl, WSARecv,
    WSARecvFrom, WSASend, WSASendTo, WSASocketW, WSAStartup, AF_INET, AF_INET6, IPPROTO_TCP, LPFN_ACCEPTEX,
    LPFN_CONNECTEX, LPFN_DISCONNECTEX, SIO_GET_EXTENSION_FUNCTION_POINTER, SIO_LOOPBACK_FAST_PATH, SOCKADDR,
    SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_STORAGE, SOCKET, SOCKET_ERROR, SOL_SOCKET, SO_PROTOCOL_INFOW,
    SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT, TCP_FASTOPEN, TCP_NODELAY, TF_REUSE_SOCKET, WSABUF,
    WSADATA, WSAEINVAL, WSAID_ACCEPTEX, WSAID_CONNECTEX, WSAID_DISCONNECTEX, WSAPROTOCOL_INFOW,
    WSA_FLAG_OVERLAPPED, WSA_IO_PENDING, XP1_IFS_HANDLES,
};
use windows_sys::Win32::Storage::FileSystem::{ReadFile, SetFileCompletionNotificationModes, WriteFile};
use windows_sys::Win32::System::IO::{
    CancelIoEx, CreateIoCompletionPort, GetOverlappedResult, GetQueuedCompletionStatusEx,
    PostQueuedCompletionStatus, OVERLAPPED, OVERLAPPED_ENTRY,
};

use super::{Completion, IoBackend, IoSlice, RawSocket, Wakeup, MAX_GATHER};
use crate::netsys;
use crate::opslab::{OpId, OpKind, OpSlab};

// FILE_SKIP_* constants live in FileSystem in some windows-sys revisions and
// are plain u8 flags; define locally to be revision-proof (values are ABI).
const FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: u8 = 0x1;
const FILE_SKIP_SET_EVENT_ON_HANDLE: u8 = 0x2;

/// Completion keys distinguishing wakeups from socket I/O. KEY_RIO marks
/// the RIO completion queue's IOCP notification (rio.rs).
const KEY_IO: usize = 1;
const KEY_WAKEUP: usize = 2;
pub(crate) const KEY_RIO: usize = 3;

/// R-030: dequeue batch size.
pub(crate) const POLL_BATCH: usize = 256;

/// R-033: free-socket pool cap.
const SOCKET_POOL_CAP: usize = 4096;

/// AcceptEx address buffer: local + remote, each sizeof(SOCKADDR_STORAGE)+16.
const ACCEPT_ADDR_LEN: u32 = size_of::<SOCKADDR_STORAGE>() as u32 + 16;
const ACCEPT_BUF_LEN: usize = 2 * ACCEPT_ADDR_LEN as usize;

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
        .map(Some)
        .map(|f| f.unwrap())
}

fn ext_fns() -> io::Result<&'static ExtFns> {
    use std::sync::OnceLock;
    static FNS: OnceLock<io::Result<ExtFns>> = OnceLock::new();
    let entry = FNS.get_or_init(|| unsafe {
        ensure_winsock();
        let probe = WSASocketW(
            AF_INET as i32,
            1, // SOCK_STREAM
            IPPROTO_TCP,
            std::ptr::null(),
            0,
            WSA_FLAG_OVERLAPPED,
        );
        if probe == !0usize {
            return Err(wsa_error());
        }
        let result = (|| {
            Ok(ExtFns {
                accept_ex: Some(load_ext_fn::<
                    unsafe extern "system" fn(
                        SOCKET,
                        SOCKET,
                        *mut core::ffi::c_void,
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
                        *const core::ffi::c_void,
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
    });
    match entry {
        Ok(f) => Ok(f),
        Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
    }
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
        id: OpId { index: 0, generation: 0 },
        socket: 0,
        accept_socket: 0,
        from_len: 0,
        dgram: Vec::new(),
        wsabufs: unsafe { zeroed() },
        addr_buf: [0; ACCEPT_BUF_LEN],
    }
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
    /// R-033: sockets recycled via DisconnectEx(TF_REUSE_SOCKET).
    free_sockets: Vec<SOCKET>,
    /// Listener -> protocol info (for creating matching accept sockets).
    listener_info: HashMap<SOCKET, WSAPROTOCOL_INFOW>,
    /// R-057 watches: fd -> (readable, writable); probes re-armed per poll.
    watches: HashMap<SOCKET, (bool, bool)>,
    /// Sockets whose probes need (re)posting at the next poll.
    watch_rearm: Vec<SOCKET>,
    /// Live probe ops -> socket, so completions translate to Ready events.
    probe_ops: HashMap<OpId, (SOCKET, bool /*write probe*/)>,
    /// Sockets where FILE_SKIP_COMPLETION_PORT_ON_SUCCESS is active.
    /// Synchronous returns may ONLY be handled inline for these (R-031);
    /// for LSP/non-IFS sockets the port still queues a completion even on
    /// immediate success, and treating it inline would double-complete.
    skip_ok: std::collections::HashSet<SOCKET>,
    /// WSASocketW flags for accept sockets. The RIO hybrid adds
    /// WSA_FLAG_REGISTERED_IO so accepted connections are RQ-capable.
    accept_socket_flags: u32,
    /// Sockets already associated with the port. A second
    /// CreateIoCompletionPort on the same (socket, port) pair fails with
    /// ERROR_INVALID_PARAMETER (found by the first Windows run: connect
    /// registers, then attach_stream registered AGAIN). Recycled sockets
    /// (R-033) keep their association across DisconnectEx, so this set +
    /// the 87-fallback below make register_socket idempotent.
    associated: std::collections::HashSet<SOCKET>,
    /// Every socket THIS PROCESS has ever associated with THIS port.
    /// Never pruned: `associated` is cleared by `detach_socket` (and by
    /// R-033 recycling), but the kernel association outlives that, so the
    /// ERROR_INVALID_PARAMETER fallback in `register_socket` needs a record
    /// that survives. Critically it also distinguishes "already associated
    /// with MY port" from "already associated with ANOTHER PROCESS's port"
    /// — a socket that arrived over a process boundary (WSADuplicateSocketW)
    /// was never associated here, and waving it through would let its
    /// completions be delivered to the owning process carrying an
    /// lpOverlapped valid only in ours (ADR-25).
    ever_associated: std::collections::HashSet<SOCKET>,
    /// ADR-25: completions dequeued carrying an OVERLAPPED outside our slab
    /// (another process's op on a handle bound to our port). Never expected
    /// to be non-zero; surfaced so it is visible rather than silent.
    pub foreign_completions: u64,
}

// SAFETY: thread-affine by loop contract; raw handles are moved with it.
unsafe impl Send for IocpBackend {}

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
            free_sockets: Vec::new(),
            ever_associated: std::collections::HashSet::new(),
            foreign_completions: 0,
            listener_info: HashMap::new(),
            watches: HashMap::new(),
            watch_rearm: Vec::new(),
            probe_ops: HashMap::new(),
            skip_ok: std::collections::HashSet::new(),
            accept_socket_flags: WSA_FLAG_OVERLAPPED | crate::netsys::WSA_FLAG_NO_HANDLE_INHERIT,
            associated: std::collections::HashSet::new(),
        })
    }

    /// May a synchronous success on this socket be handled inline (R-031)?
    fn inline_ok(&self, socket: SOCKET) -> bool {
        self.skip_ok.contains(&socket)
    }

    fn rearm_watch_probes(&mut self) {
        if self.watch_rearm.is_empty() {
            return;
        }
        let sockets: Vec<SOCKET> = self.watch_rearm.drain(..).collect();
        for s in sockets {
            let Some(&(r, w)) = self.watches.get(&s) else { continue };
            let has_r = self.probe_ops.values().any(|&(ps, pw)| ps == s && !pw);
            let has_w = self.probe_ops.values().any(|&(ps, pw)| ps == s && pw);
            if r && !has_r {
                let _ = self.post_probe(s, false);
            }
            if w && !has_w {
                let _ = self.post_probe(s, true);
            }
        }
    }

    /// Zero-byte recv/send probe (R-057 readiness emulation).
    fn post_probe(&mut self, socket: SOCKET, write: bool) -> io::Result<()> {
        let id = self.new_op(if write { OpKind::Send } else { OpKind::Recv }, socket);
        let op = &mut self.slab.get_mut(id).unwrap().data;
        op.wsabufs[0] = WSABUF { len: 0, buf: op.addr_buf.as_mut_ptr() };
        let wsabuf_ptr = &mut op.wsabufs[0] as *mut WSABUF;
        let ov = &mut op.overlapped as *mut OVERLAPPED;
        let mut bytes: u32 = 0;
        let mut flags: u32 = 0;
        let rc = if write {
            unsafe { WSASend(socket, wsabuf_ptr, 1, &mut bytes, 0, ov, None) }
        } else {
            unsafe { WSARecv(socket, wsabuf_ptr, 1, &mut bytes, &mut flags, ov, None) }
        };
        if rc == 0 && self.inline_ok(socket) {
            self.slab.complete(id);
            self.slab.release(id);
            self.inline_completions.push(Completion::Ready { socket, readable: !write, writable: write });
            // Next poll re-arms (level-trigger cadence).
            self.watch_rearm.push(socket);
            return Ok(());
        }
        if rc == 0 {
            // Skip-modes off: the completion still arrives via the port.
            self.probe_ops.insert(id, (socket, write));
            return Ok(());
        }
        let err = unsafe { WSAGetLastError() };
        if err == WSA_IO_PENDING {
            self.probe_ops.insert(id, (socket, write));
            return Ok(());
        }
        // Probe failed outright (e.g. connection reset, or WSAENOTCONN on
        // a still-connecting socket): report readiness so the callback
        // runs and observes the error from its own syscall — and re-arm,
        // preserving level-trigger semantics for callbacks that decide
        // the fd is not actually actionable yet (sock_connect's
        // in-progress guard relies on the watch firing again).
        self.slab.complete(id);
        self.slab.release(id);
        self.inline_completions.push(Completion::Ready { socket, readable: !write, writable: write });
        self.watch_rearm.push(socket);
        Ok(())
    }

    /// Allocate + initialize an op slot; returns (id, overlapped ptr).
    fn new_op(&mut self, kind: OpKind, socket: SOCKET) -> OpId {
        let id = self.slab.post(kind);
        let op = &mut self.slab.get_mut(id).expect("fresh id resolves").data;
        op.overlapped = unsafe { zeroed() };
        op.id = id;
        op.socket = socket;
        op.accept_socket = !0usize;
        id
    }

    fn overlapped_ptr(&mut self, id: OpId) -> *mut OVERLAPPED {
        &mut self.slab.get_mut(id).expect("live op").data.overlapped
    }

    /// Handle a post that returned synchronously under
    /// FILE_SKIP_COMPLETION_PORT_ON_SUCCESS: surface inline, free the slot
    /// (R-031: no queued completion will follow).
    fn complete_inline(&mut self, id: OpId, bytes: u32) {
        self.slab.complete(id);
        self.slab.release(id);
        self.syscalls_saved_inline += 1;
        self.inline_completions.push(Completion::Io { op: id, bytes, os_error: 0 });
    }

    /// Post failed synchronously: reclaim the slot, return the error.
    fn fail_post(&mut self, id: OpId, err: io::Error) -> io::Error {
        self.slab.complete(id);
        self.slab.release(id);
        err
    }

    fn accept_socket_for(&mut self, listener: SOCKET) -> io::Result<SOCKET> {
        // R-033: prefer recycled sockets.
        if let Some(s) = self.free_sockets.pop() {
            return Ok(s);
        }
        let info = match self.listener_info.get(&listener) {
            Some(info) => *info,
            None => {
                let mut info: WSAPROTOCOL_INFOW = unsafe { zeroed() };
                let mut len = size_of::<WSAPROTOCOL_INFOW>() as i32;
                let rc = unsafe {
                    getsockopt(
                        listener,
                        SOL_SOCKET,
                        SO_PROTOCOL_INFOW,
                        (&mut info as *mut WSAPROTOCOL_INFOW).cast(),
                        &mut len,
                    )
                };
                if rc == SOCKET_ERROR {
                    return Err(wsa_error());
                }
                self.listener_info.insert(listener, info);
                info
            }
        };
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
        if s == !0usize {
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
            if s == !0usize {
                return Err(wsa_error());
            }
        }
        if self.accept_socket_flags & crate::netsys::WSA_FLAG_NO_HANDLE_INHERIT == 0 {
            crate::netsys::clear_handle_inherit(s);
        }
        Ok(s)
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

    /// Pre-poll work shared with the hybrid: re-arm readiness probes and
    /// surface inline (never-queued) completions.
    pub(crate) fn pre_poll(&mut self, out: &mut Vec<Completion>) {
        self.rearm_watch_probes();
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
        let ifs = rc == 0 && (info.dwServiceFlags1 & XP1_IFS_HANDLES) != 0;
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
        if let Some((socket, write_probe)) = self.probe_ops.remove(&id) {
            self.slab.complete(id);
            self.slab.release(id);
            out.push(Completion::Ready { socket, readable: !write_probe, writable: write_probe });
            self.watch_rearm.push(socket); // next poll re-arms if still watched
            return;
        }
        let Some((kind, _was_cancelled)) = self.slab.complete(id) else {
            return; // stale/duplicate completion; debug_assert'ed in slab
        };
        // Pipe ops use plain Win32 HANDLEs, not Winsock SOCKETs —
        // WSAGetOverlappedResult only works on the latter (R-051).
        let (bytes, os_error) = if matches!(kind, OpKind::PipeRead | OpKind::PipeWrite) {
            unsafe {
                let handle = (*op_ptr).socket as HANDLE;
                let mut bytes: u32 = 0;
                let ok = GetOverlappedResult(handle, entry.lpOverlapped, &mut bytes, 0);
                if ok != 0 {
                    (bytes, 0u32)
                } else {
                    (0, GetLastError())
                }
            }
        } else {
            unsafe {
                let socket = (*op_ptr).socket;
                let mut bytes: u32 = 0;
                let mut flags: u32 = 0;
                let ok = WSAGetOverlappedResult(socket, entry.lpOverlapped, &mut bytes, 0, &mut flags);
                if ok != 0 {
                    (bytes, 0u32)
                } else {
                    (0, WSAGetLastError() as u32)
                }
            }
        };
        match kind {
            OpKind::Accept => {
                // Keep the slot alive: the accepted socket + addr buffer are
                // consumed via take_accept_socket (R-032).
                if os_error != 0 {
                    let s = unsafe { (*op_ptr).accept_socket };
                    if s != !0usize {
                        unsafe { closesocket(s) };
                    }
                    self.slab.release(id);
                }
            }
            OpKind::Disconnect => {
                // R-033: recycle into the free-socket pool (cap, overflow ->
                // closesocket).
                let s = unsafe { (*op_ptr).socket };
                if os_error == 0 && self.free_sockets.len() < SOCKET_POOL_CAP {
                    self.free_sockets.push(s);
                } else {
                    unsafe { closesocket(s) };
                }
                self.slab.release(id);
            }
            OpKind::Connect => {
                if os_error == 0 {
                    // R-034: SO_UPDATE_CONNECT_CONTEXT on completion.
                    unsafe {
                        setsockopt(
                            (*op_ptr).socket,
                            SOL_SOCKET,
                            SO_UPDATE_CONNECT_CONTEXT,
                            std::ptr::null(),
                            0,
                        );
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
}

impl IoBackend for IocpBackend {
    fn foreign_completions(&self) -> u64 {
        self.foreign_completions
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
            // associated" (e.g. an R-033 recycled socket whose association
            // outlived our bookkeeping): treat as success.
            //
            // DANGEROUS when the socket crossed a PROCESS boundary: a file
            // object binds to exactly one IOCP for life, so a listener
            // duplicated into N workers (R-090) can only ever be associated
            // by whichever worker got there first. The losers land here and
            // this heuristic — which cannot tell "already mine" from
            // "already someone else's" — waves them through. They then post
            // AcceptEx whose completions are delivered to the WINNER's port,
            // carrying an lpOverlapped that is only valid in the loser's
            // address space; translate_entry dereferences it and takes an
            // access violation. See ADR-24.
            if err.raw_os_error() == Some(87) {
                let ours = self.ever_associated.contains(&socket);
                if trace_assoc_enabled() {
                    eprintln!(
                        "cadeloop-iocp: register_socket({socket}) ERROR_INVALID_PARAMETER; ours={ours}"
                    );
                    let _ = std::io::Write::flush(&mut std::io::stderr());
                }
                if !ours {
                    // Associated with a port we do not own — almost always a
                    // socket that crossed a process boundary. Proceeding
                    // would post ops whose completions land on the OWNING
                    // process's port carrying a pointer into ours (ADR-25):
                    // a memory-safety violation, reported as a crash far
                    // from here. Fail loudly at setup instead.
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "socket is already bound to a completion port this process does not own \
                         (a Windows file object binds to exactly one IOCP for life; a listener \
                         duplicated into several processes can only be driven by one of them)",
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
                    self.associated.insert(socket);
                    self.apply_skip_modes(socket);
                    return Ok(());
                }
            }
            return Err(err);
        }
        self.associated.insert(socket);
        self.ever_associated.insert(socket);
        self.apply_skip_modes(socket);
        Ok(())
    }

    /// R-051: associate a named-pipe HANDLE with the same port sockets
    /// use. No skip-modes here — every pipe completion (sync or async)
    /// still posts to the port, which keeps post_pipe_read/write simple
    /// (no inline fast path to reason about; pipes are not the hot path).
    fn register_pipe(&mut self, handle: RawSocket) -> io::Result<()> {
        // Deliberately NOT short-circuited on `associated`, for exactly the
        // reason register_socket is not: the set is keyed by HANDLE VALUE
        // and Windows recycles those. A closed pipe's value left behind
        // makes the next handle given that number look associated when it
        // is not, and its overlapped ReadFile/WriteFile completion is then
        // delivered nowhere -- the future hangs forever with no error.
        // Fixing this for sockets and not for pipes was an oversight.
        let rc = unsafe { CreateIoCompletionPort(handle as HANDLE, self.port.0, KEY_IO, 0) };
        if rc.is_null() {
            return Err(win_error());
        }
        self.associated.insert(handle);
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
        let ov = self.overlapped_ptr(id);
        let mut read: u32 = 0;
        let ok = unsafe { ReadFile(handle as HANDLE, buf, len, &mut read, ov) };
        if ok == 0 {
            let err = unsafe { GetLastError() };
            if err != ERROR_IO_PENDING {
                return Err(self.fail_post(id, io::Error::from_raw_os_error(err as i32)));
            }
        }
        // Synchronous success (ok != 0) still posts a completion packet
        // (no skip-modes on pipe handles) — same code path as pending.
        Ok(id)
    }

    fn post_pipe_write(&mut self, handle: RawSocket, data: &[u8]) -> io::Result<OpId> {
        let id = self.new_op(OpKind::PipeWrite, handle);
        let ov = self.overlapped_ptr(id);
        let mut written: u32 = 0;
        let ok = unsafe { WriteFile(handle as HANDLE, data.as_ptr(), data.len() as u32, &mut written, ov) };
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
        {
            let op = &mut self.slab.get_mut(id).unwrap().data;
            op.accept_socket = accept_socket;
        }
        let ov = self.overlapped_ptr(id);
        let op = &mut self.slab.get_mut(id).unwrap().data;
        let mut received: u32 = 0;
        let ok = unsafe {
            (fns.accept_ex.unwrap())(
                listener,
                accept_socket,
                op.addr_buf.as_mut_ptr().cast(),
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
                self.slab.complete(id);
                self.syscalls_saved_inline += 1;
                self.inline_completions.push(Completion::Io { op: id, bytes: 0, os_error: 0 });
            }
            return Ok(id);
        }
        let err = unsafe { WSAGetLastError() };
        if err == WSA_IO_PENDING || err as u32 == ERROR_IO_PENDING {
            return Ok(id);
        }
        unsafe { closesocket(accept_socket) };
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
                // take_recv_from_addr) — complete_inline would release it.
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
        let addr = self.slab.get(op).and_then(|s| {
            let n = s.data.from_len.clamp(0, ACCEPT_BUF_LEN as i32) as usize;
            netsys::parse_any_sockaddr(&s.data.addr_buf[..n])
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
        let n = bufs.len().min(MAX_GATHER);
        for (i, b) in bufs.iter().take(n).enumerate() {
            op.wsabufs[i] = WSABUF { len: b.len, buf: b.ptr as *mut u8 };
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
        // R-034: ConnectEx requires a bound socket; bind to wildcard :0.
        // WSAEINVAL from an already-bound socket is tolerated.
        unsafe {
            let family = (*(addr.as_ptr() as *const SOCKADDR)).sa_family;
            if family == AF_INET {
                let mut local: SOCKADDR_IN = zeroed();
                local.sin_family = AF_INET;
                bind(socket, (&local as *const SOCKADDR_IN).cast(), size_of::<SOCKADDR_IN>() as i32);
            } else if family == AF_INET6 {
                let mut local: SOCKADDR_IN6 = zeroed();
                local.sin6_family = AF_INET6;
                bind(socket, (&local as *const SOCKADDR_IN6).cast(), size_of::<SOCKADDR_IN6>() as i32);
            }
        }
        let id = self.new_op(OpKind::Connect, socket);
        let ov = self.overlapped_ptr(id);
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
        let ov = self.overlapped_ptr(id);
        let ok = unsafe { (fns.disconnect_ex.unwrap())(socket, ov, TF_REUSE_SOCKET, 0) };
        if ok != 0 && self.inline_ok(socket) {
            // Synchronous: recycle immediately (R-033).
            self.slab.complete(id);
            self.slab.release(id);
            self.syscalls_saved_inline += 1;
            if self.free_sockets.len() < SOCKET_POOL_CAP {
                self.free_sockets.push(socket);
            } else {
                unsafe { closesocket(socket) };
            }
            self.inline_completions.push(Completion::Io { op: id, bytes: 0, os_error: 0 });
            return Ok(id);
        }
        let err = unsafe { WSAGetLastError() };
        if err == WSA_IO_PENDING || err as u32 == ERROR_IO_PENDING {
            return Ok(id);
        }
        Err(self.fail_post(id, io::Error::from_raw_os_error(err)))
    }

    /// R-057 readiness emulation: readable = pending zero-byte WSARecv
    /// probe (completes when data or EOF arrives); writable = zero-byte
    /// WSASend probe (documented level-trigger approximation — a connected
    /// socket with sndbuf space completes immediately, so writable watches
    /// fire once per poll cycle like epoll level-triggering). Probes are
    /// re-armed at the top of each poll for fds still watched.
    fn set_watch(&mut self, socket: RawSocket, readable: bool, writable: bool) -> io::Result<()> {
        if readable || writable {
            // Watched sockets usually arrive via add_reader/add_writer on
            // fds Python created itself (never wired through us), so they
            // are not yet associated with the port — and a probe posted on
            // an unassociated socket completes into the void, hanging the
            // watch forever (run-4: aiohttp sock_connect timeout).
            self.register_socket(socket)?;
            self.watches.insert(socket, (readable, writable));
        } else {
            self.watches.remove(&socket);
        }
        self.watch_rearm.push(socket);
        Ok(())
    }

    fn detach_socket(&mut self, socket: RawSocket) {
        self.associated.remove(&socket);
        self.skip_ok.remove(&socket); // fd-number reuse must not inherit skip modes
        self.watches.remove(&socket);
        self.listener_info.remove(&socket);
        // In-flight ops on the socket deliver ABORTED completions via the
        // port after closesocket; the slab reaps them there (R-037).
    }

    fn cancel(&mut self, op: OpId) -> io::Result<()> {
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
        let (accepted, listener) = {
            let slot = self
                .slab
                .get_mut(op)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "accept op not live"))?;
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
        self.slab.release(op);
        if rc == SOCKET_ERROR {
            let err = wsa_error();
            unsafe { closesocket(accepted) };
            return Err(err);
        }
        Ok(accepted)
    }

    fn poll(&mut self, out: &mut Vec<Completion>, timeout: Option<Duration>) -> io::Result<usize> {
        let before = out.len();
        self.rearm_watch_probes();
        // R-031: inline completions first — they were never queued.
        if !self.inline_completions.is_empty() {
            out.append(&mut self.inline_completions);
        }
        let timeout_ms: u32 = match timeout {
            _ if out.len() > before => 0, // work already available
            Some(t) => crate::backend::wait_millis(t).min(u32::MAX as u128) as u32,
            None => 0,
        };
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
                return Ok(out.len() - before);
            }
            return Err(io::Error::from_raw_os_error(err as i32));
        }
        for i in 0..n as usize {
            let entry = self.entries[i];
            self.translate_entry(&entry, out);
        }
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
