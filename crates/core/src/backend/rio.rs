//! Registered I/O backend scaffold (R-040..R-044 — milestone M3).
//!
//! What exists now: availability probing via
//! `SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER` / `WSAID_MULTIPLE_RIO`
//! (the R-020 auto-selection probe) and the design notes below. The backend
//! itself reports `Unsupported` until M3, so `backend="auto"` resolves to
//! IOCP; `backend="rio"` fails loudly rather than silently degrading.
//!
//! M3 design (per spec):
//! * One RIOCQ per loop, `cfg.rio_cq_size` (default 65536), event mode
//!   `RIONotify` integrated with the IOCP wait via
//!   `RIO_IOCP_COMPLETION`; the spin phase (R-060) calls
//!   `RIODequeueCompletion` directly before arming RIONotify (R-041).
//! * Per-socket RIORQs sized `cfg.rio_rq_recv`/`rio_rq_send` (default
//!   32/32); accept/connect remain AcceptEx/ConnectEx, then the socket is
//!   upgraded into an RQ (R-042).
//! * All buffers from the pre-registered slabs of `buffers::BufferPool`;
//!   `RIORegisterBuffer` once per slab region at creation, never per-op
//!   (R-043) — the `regions_mut()` hook and per-region cookie already
//!   exist for this.
//! * Sends enqueued with `RIO_MSG_DEFER`, one commit per tick to amortize
//!   doorbells; dequeue batch 1024 (R-044).
//! * CQ overflow: fatal-log + stop posting recvs (backpressure), never
//!   silent loss (§16).

use std::io;
use std::mem::{size_of, zeroed};

use windows_sys::core::GUID;
use windows_sys::Win32::Networking::WinSock::{
    closesocket, WSAGetLastError, WSAIoctl, WSASocketW, AF_INET,
    RIO_EXTENSION_FUNCTION_TABLE, SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER, SOCKET_ERROR,
    WSAID_MULTIPLE_RIO, WSA_FLAG_OVERLAPPED, IPPROTO_TCP,
};

use super::IoBackend;

/// R-020 auto-probe: is the RIO function table resolvable?
pub fn probe_available() -> bool {
    resolve_table().is_ok()
}

fn resolve_table() -> io::Result<RIO_EXTENSION_FUNCTION_TABLE> {
    super::iocp::ensure_winsock();
    unsafe {
        let probe = WSASocketW(
            AF_INET as i32,
            1, // SOCK_STREAM
            IPPROTO_TCP as i32,
            std::ptr::null(),
            0,
            WSA_FLAG_OVERLAPPED,
        );
        if probe == !0usize {
            return Err(io::Error::from_raw_os_error(WSAGetLastError()));
        }
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
        closesocket(probe);
        if rc == SOCKET_ERROR {
            return Err(io::Error::from_raw_os_error(WSAGetLastError()));
        }
        Ok(table)
    }
}

pub struct RioBackend {
    _table: RIO_EXTENSION_FUNCTION_TABLE,
}

impl RioBackend {
    pub fn new() -> io::Result<Self> {
        // Probe so the error distinguishes "no RIO on this OS" from
        // "not implemented yet".
        let _table = resolve_table().map_err(|e| {
            io::Error::new(e.kind(), format!("RIO unavailable on this system: {e}"))
        })?;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the RIO backend lands in milestone M3; use backend=\"auto\" or \"iocp\"",
        ))
    }
}

// The impl exists so the type participates in the backend factory once M3
// fills it in; unreachable until `new()` can succeed.
impl IoBackend for RioBackend {
    fn post_accept(&mut self, _: super::RawSocket) -> io::Result<crate::opslab::OpId> {
        unreachable!("RioBackend cannot be constructed before M3")
    }
    fn post_recv(&mut self, _: super::RawSocket, _: *mut u8, _: u32) -> io::Result<crate::opslab::OpId> {
        unreachable!("RioBackend cannot be constructed before M3")
    }
    fn post_send(&mut self, _: super::RawSocket, _: &[super::IoSlice]) -> io::Result<crate::opslab::OpId> {
        unreachable!("RioBackend cannot be constructed before M3")
    }
    fn post_connect(&mut self, _: super::RawSocket, _: &[u8]) -> io::Result<crate::opslab::OpId> {
        unreachable!("RioBackend cannot be constructed before M3")
    }
    fn post_disconnect_reuse(&mut self, _: super::RawSocket) -> io::Result<crate::opslab::OpId> {
        unreachable!("RioBackend cannot be constructed before M3")
    }
    fn cancel(&mut self, _: crate::opslab::OpId) -> io::Result<()> {
        unreachable!("RioBackend cannot be constructed before M3")
    }
    fn poll(
        &mut self,
        _: &mut Vec<super::Completion>,
        _: Option<std::time::Duration>,
    ) -> io::Result<usize> {
        unreachable!("RioBackend cannot be constructed before M3")
    }
    fn wakeup_handle(&self) -> std::sync::Arc<dyn super::Wakeup> {
        unreachable!("RioBackend cannot be constructed before M3")
    }
    fn name(&self) -> &'static str {
        "rio"
    }
}
