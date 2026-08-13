//! L0 kernel I/O backend interface (R-020).
//!
//! One internal trait, [`IoBackend`], implemented by:
//!
//! * [`iocp`] — production backend, Windows IOCP (R-030..R-038);
//! * [`rio`] — production backend, Windows Registered I/O (R-040..R-044,
//!   milestone M3; currently a scaffold that reports unavailability);
//! * [`portable`] — dev/test backend for non-Windows hosts. It supports
//!   only completion-queue + wakeup semantics (timers, `call_soon`,
//!   `call_soon_threadsafe`) so the L1/L3 layers can be developed and
//!   conformance-tested anywhere. It implements no socket ops and is never
//!   published in wheels (wheels are cp311-win_amd64 only, R-110).
//!
//! Backend selection at loop creation (R-020): `"auto" | "iocp" | "rio" |
//! "epoll"`, where auto probes RIO availability and falls back to IOCP.
//! `"epoll"` is the Linux dev backend's explicit name (Linux-only).

use std::io;
use std::time::Duration;

use crate::opslab::OpId;

#[cfg(target_os = "linux")]
pub mod epoll;
#[cfg(windows)]
pub mod iocp;
#[cfg(not(any(windows, target_os = "linux")))]
pub mod portable;
#[cfg(windows)]
pub mod rio;
pub mod rio_util;

/// A reaped completion, translated out of backend-specific form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    /// Cross-thread wakeup (R-022) or timer-park interruption; carries no
    /// payload — the reactor drains the cross-thread queue after any poll.
    Wakeup,
    /// A kernel I/O op finished. `bytes` is the transfer count;
    /// `os_error` is the WSA/Win32 (or errno on the epoll backend) error
    /// code when the op failed (0 == success). Errno mapping to Python
    /// OSError happens in pyshim.
    Io { op: OpId, bytes: u32, os_error: u32 },
    /// Level-triggered readiness for a watched fd (R-057:
    /// add_reader/add_writer and the sock_* surface). Delivered every poll
    /// while the condition holds and the watch is armed.
    Ready { socket: RawSocket, readable: bool, writable: bool },
}

/// Socket handle abstraction: SOCKET (usize) on Windows; unused by the
/// portable backend.
pub type RawSocket = usize;

/// Does this `Completion::Io::os_error` mean "op cancelled by us"
/// (CancelIoEx / ECANCELED), as opposed to a real transport error?
pub fn is_cancelled_error(code: u32) -> bool {
    #[cfg(windows)]
    {
        code == 995 // ERROR_OPERATION_ABORTED / WSA_OPERATION_ABORTED
    }
    #[cfg(not(windows))]
    {
        code == 125 // ECANCELED
    }
}

/// Gather list entry for scatter/gather sends (R-035: up to 16 per send).
#[derive(Debug, Clone, Copy)]
pub struct IoSlice {
    pub ptr: *const u8,
    pub len: u32,
}

pub const MAX_GATHER: usize = 16;

/// R-020 op surface. Ops return an `OpId` whose completion is later
/// delivered by `poll`. Backends that cannot support an op return
/// `io::ErrorKind::Unsupported`.
///
/// Buffer registration (`register_buffers`/`deregister_buffers`, R-043) is
/// a no-op on IOCP and required on RIO; the reactor calls it whenever the
/// buffer pool grows a slab.
pub trait IoBackend {
    /// Associate a socket with the completion mechanism before posting ops
    /// on it. On IOCP this binds the handle to the port and applies
    /// `SetFileCompletionNotificationModes` (R-031, with the LSP guard).
    fn register_socket(&mut self, _socket: RawSocket) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "register_socket"))
    }

    /// Fetch the accepted socket of a completed accept op and release the
    /// op slot. Applies `SO_UPDATE_ACCEPT_CONTEXT` (R-032).
    fn take_accept_socket(&mut self, _op: OpId) -> io::Result<RawSocket> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "take_accept_socket"))
    }

    fn post_accept(&mut self, listener: RawSocket) -> io::Result<OpId>;
    fn post_recv(&mut self, socket: RawSocket, buf: *mut u8, len: u32) -> io::Result<OpId>;
    fn post_send(&mut self, socket: RawSocket, bufs: &[IoSlice]) -> io::Result<OpId>;
    fn post_connect(&mut self, socket: RawSocket, addr: &[u8]) -> io::Result<OpId>;
    fn post_disconnect_reuse(&mut self, socket: RawSocket) -> io::Result<OpId>;

    // ---- datagrams (R-058) — Unsupported by default (portable) --------

    fn post_recv_from(&mut self, _socket: RawSocket, _buf: *mut u8, _len: u32) -> io::Result<OpId> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "datagrams unsupported on this backend"))
    }

    /// Consume the peer address of a completed recv_from op — releases
    /// the op slot (the accept-socket lifecycle, applied to datagrams).
    fn take_recv_from_addr(&mut self, _op: OpId) -> Option<std::net::SocketAddr> {
        None
    }

    /// The datagram is COPIED into the op (small payloads; no caller
    /// buffer pinning). `addr` None = connected-mode send().
    fn post_send_to(
        &mut self,
        _socket: RawSocket,
        _data: &[u8],
        _addr: Option<&std::net::SocketAddr>,
    ) -> io::Result<OpId> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "datagrams unsupported on this backend"))
    }

    /// Request cancellation of an in-flight op. MUST tolerate the op having
    /// already completed (ERROR_NOT_FOUND) — the completion is still
    /// delivered via `poll` either way (R-037).
    fn cancel(&mut self, op: OpId) -> io::Result<()>;

    /// Arm/disarm level-triggered readiness watches for a raw fd (R-057).
    /// `readable`/`writable` express the DESIRED watch set (both false =
    /// fully unwatched). On IOCP this is emulated with zero-byte probe ops;
    /// on epoll it is native interest.
    fn set_watch(&mut self, _socket: RawSocket, _readable: bool, _writable: bool) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "set_watch"))
    }

    /// Forget all backend state for a socket (pending-op bookkeeping,
    /// watches, interest registrations) ahead of `closesocket`/`close`.
    /// In-flight ops on the socket still deliver their (failed/aborted)
    /// completions where the OS queues them.
    fn detach_socket(&mut self, _socket: RawSocket) {}

    /// Reap up to `out.capacity()` completions, blocking up to `timeout`
    /// (None = non-blocking). Returns the number appended to `out`.
    /// Called with the GIL released (R-021).
    fn poll(&mut self, out: &mut Vec<Completion>, timeout: Option<Duration>) -> io::Result<usize>;

    /// Non-blocking poll used by the spin phase of spin-then-park (R-060).
    fn try_poll(&mut self, out: &mut Vec<Completion>) -> io::Result<usize> {
        self.poll(out, Some(Duration::ZERO))
    }

    fn register_buffers(&mut self, _regions: &mut [(*mut u8, usize, &mut Option<u64>)]) -> io::Result<()> {
        Ok(())
    }
    fn deregister_buffers(&mut self, _cookies: &[u64]) -> io::Result<()> {
        Ok(())
    }

    /// A cheap, cloneable, thread-safe wakeup poster (R-022):
    /// `PostQueuedCompletionStatus` on IOCP, condvar notify on the portable
    /// backend.
    fn wakeup_handle(&self) -> std::sync::Arc<dyn Wakeup>;

    /// Human-readable backend name for stats/diagnostics.
    fn name(&self) -> &'static str;

    /// Backend diagnostic counters (R-103): RIO reports
    /// (notifications_received, watchdog_reaps); None elsewhere.
    fn diag(&self) -> Option<(u64, u64)> {
        None
    }
}

/// Cross-thread wakeup poster. Must be safe to call from any thread and
/// after (or concurrently with) loop shutdown.
pub trait Wakeup: Send + Sync {
    fn wake(&self);
}

/// Requested backend kind (R-020 `backend="auto"|"iocp"|"rio"|"epoll"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Auto,
    Iocp,
    Rio,
    Epoll,
}

impl BackendKind {
    pub fn parse(s: &str) -> Option<BackendKind> {
        match s {
            "auto" => Some(BackendKind::Auto),
            "iocp" => Some(BackendKind::Iocp),
            "rio" => Some(BackendKind::Rio),
            "epoll" => Some(BackendKind::Epoll),
            _ => None,
        }
    }
}

/// Backend construction options (R-041/R-042 RIO sizing; ignored by
/// backends that have no use for them).
#[derive(Debug, Clone, Copy)]
pub struct BackendOptions {
    pub rio_cq_size: u32,
    pub rio_rq_recv: u32,
    pub rio_rq_send: u32,
}

impl Default for BackendOptions {
    fn default() -> Self {
        BackendOptions { rio_cq_size: 65536, rio_rq_recv: 32, rio_rq_send: 32 }
    }
}

/// Instantiate the platform backend.
///
/// On Windows: `"rio"` builds the Registered I/O hybrid (R-040..R-044).
/// `"auto"` resolves to IOCP for now — the RIO machinery is implemented
/// and compile-verified, but its behavioral validation is the remaining
/// M3 Windows-hardware gate; auto flips to probe-RIO-first once that
/// lands. On other platforms every kind resolves to the dev backend so
/// loop-semantics tests run unmodified everywhere.
pub fn create(kind: BackendKind, opts: &BackendOptions) -> io::Result<Box<dyn IoBackend + Send>> {
    #[cfg(windows)]
    {
        match kind {
            BackendKind::Iocp | BackendKind::Auto => Ok(Box::new(iocp::IocpBackend::new()?)),
            BackendKind::Rio => Ok(Box::new(rio::RioBackend::new(
                opts.rio_cq_size,
                opts.rio_rq_recv,
                opts.rio_rq_send,
            )?)),
            BackendKind::Epoll => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "epoll backend is Linux-only (use 'auto', 'iocp' or 'rio')",
            )),
        }
    }
    #[cfg(target_os = "linux")]
    {
        // Iocp/Rio deliberately resolve to the dev backend too, so
        // backend-parameterized test sweeps run unmodified everywhere.
        let _ = (kind, opts);
        Ok(Box::new(epoll::EpollBackend::new()?))
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = (kind, opts);
        Ok(Box::new(portable::PortableBackend::new()))
    }
}
