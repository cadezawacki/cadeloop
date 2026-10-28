//! Platform-independent logic for the IOCP backend.
//!
//! The same discipline as `rio_util`: everything the IOCP backend's
//! correctness hinges on that is pure logic -- how a completion's NTSTATUS
//! becomes the error code upper layers test, how AFD poll events become
//! readiness, when a slot retired without a port completion may be
//! recycled, how recycled sockets are pooled -- lives here, unit-tested on
//! every development platform. `iocp.rs` keeps only the FFI.

use std::io;
use std::sync::OnceLock;

// ---- NTSTATUS ---------------------------------------------------------- //

pub const STATUS_SUCCESS: i32 = 0;
pub const STATUS_PENDING: i32 = 0x103;
pub const STATUS_BUFFER_OVERFLOW: i32 = 0x8000_0005_u32 as i32;
pub const STATUS_CANCELLED: i32 = 0xC000_0120_u32 as i32;
pub const STATUS_PIPE_BROKEN: i32 = 0xC000_014B_u32 as i32;

/// `NT_SUCCESS`: success and informational codes. Warnings are not.
#[inline]
pub fn nt_success(status: i32) -> bool {
    status >= 0
}

/// `NT_ERROR` (severity 3). A request that fails with one of these
/// synchronously was never pended, so the I/O manager queues no completion
/// packet for it. A WARNING (severity 2, e.g. STATUS_BUFFER_OVERFLOW) is not
/// an error in that sense: its packet IS queued even though the Win32 call
/// that issued it returned FALSE.
#[inline]
pub fn nt_error(status: i32) -> bool {
    (status as u32) >> 30 == 3
}

/// The low 32 bits of `OVERLAPPED.Internal`: where the I/O manager writes
/// the request's final NTSTATUS (the field is the `Status` half of the
/// `IO_STATUS_BLOCK` the OVERLAPPED begins with).
#[inline]
pub fn overlapped_status(internal: usize) -> i32 {
    internal as u32 as i32
}

/// The Winsock error a failed socket completion reports, for the statuses
/// it can carry -- what `WSAGetOverlappedResult` would have returned, which
/// is what everything above this layer tests (`is_cancelled_error`,
/// `is_transient_accept_error`, the Python exception class).
///
/// Decoding the OVERLAPPED directly instead of calling
/// `WSAGetOverlappedResult` matters because that call needs a LIVE socket
/// handle: most cancelled ops are reaped after `closesocket`, where it fails
/// WSAENOTSOCK and reports neither the real status nor the byte count, and
/// with a recycled handle value it dispatches into whatever provider now
/// owns the number.
///
/// `None` for statuses outside the table; the caller falls back to
/// `RtlNtStatusToDosError`. Table after msafd's and libuv's
/// (`uv_ntstatus_to_winsock_error`), except that cancellation is
/// WSA_OPERATION_ABORTED as msafd reports it, not libuv's WSAEINTR.
pub fn ntstatus_to_wsa(status: i32) -> Option<u32> {
    const WSA_OPERATION_ABORTED: u32 = 995;
    let code = match status as u32 {
        0xC000_0008 | 0xC000_0024 => 10038, // INVALID_HANDLE, OBJECT_TYPE_MISMATCH -> WSAENOTSOCK
        0xC000_009A | 0xC000_0007 | 0xC000_012D | 0xC000_00A1 | 0xC000_0017 | 0xC000_0044 | 0xC000_0097
        | 0xC000_013D => 10055, // resource exhaustion -> WSAENOBUFS
        0xC000_0209 | 0xC000_0043 | 0xC000_020A => 10048, // -> WSAEADDRINUSE
        0xC000_013F | 0xC000_00B5 => 10060, // LINK_TIMEOUT, IO_TIMEOUT -> WSAETIMEDOUT
        0xC000_0237 => 10101,               // GRACEFUL_DISCONNECT -> WSAEDISCON
        0xC000_013C | 0xC000_020D | 0xC000_013E | 0xC000_020C | 0xC000_023F | 0xC000_A012 => 10054, // -> WSAECONNRESET
        0xC000_013B | 0xC000_020F | 0xC000_0241 => 10053, // -> WSAECONNABORTED
        0xC000_00BE | 0xC000_023C | 0xC000_023E => 10051, // -> WSAENETUNREACH
        0xC000_023D => 10065,                             // HOST_UNREACHABLE -> WSAEHOSTUNREACH
        0xC000_0120 | 0xC000_0240 => WSA_OPERATION_ABORTED, // CANCELLED, REQUEST_ABORTED
        0x8000_0005 | 0xC000_0206 => 10040, // BUFFER_OVERFLOW, INVALID_BUFFER_SIZE -> WSAEMSGSIZE
        0xC000_0023 | 0xC000_0005 => 10014, // BUFFER_TOO_SMALL, ACCESS_VIOLATION -> WSAEFAULT
        0xC000_00A3 | 0xC000_00D0 => 10035, // -> WSAEWOULDBLOCK
        0xC000_00C3 | 0xC000_00BF | 0xC000_000E | 0xC000_000F | 0xC000_003A | 0xC000_0034 | 0xC000_00C4 => {
            10050 // -> WSAENETDOWN
        }
        0xC000_0140 => 10057,               // INVALID_CONNECTION -> WSAENOTCONN
        0xC000_00BC | 0xC000_0236 => 10061, // REMOTE_NOT_LISTENING, CONNECTION_REFUSED -> WSAECONNREFUSED
        0xC000_00B0 => 10058,               // PIPE_DISCONNECTED -> WSAESHUTDOWN
        0xC000_0018 | 0xC000_0141 | 0xC000_0207 => 10049, // -> WSAEADDRNOTAVAIL
        0xC000_00BB | 0xC000_0002 => 10045, // NOT_SUPPORTED, NOT_IMPLEMENTED -> WSAEOPNOTSUPP
        0xC000_0022 => 10013,               // ACCESS_DENIED -> WSAEACCES
        _ => return None,
    };
    Some(code)
}

// ---- AFD poll (R-057 readiness) ---------------------------------------- //

/// `IOCTL_AFD_POLL`: CTL_CODE(FILE_DEVICE_NETWORK, 9, METHOD_BUFFERED,
/// FILE_ANY_ACCESS). Undocumented but stable since NT 4; libuv's uv_poll,
/// mio (and so tokio) and wepoll all build readiness on it.
pub const IOCTL_AFD_POLL: u32 = 0x0001_2024;

pub const AFD_POLL_RECEIVE: u32 = 0x0001;
pub const AFD_POLL_SEND: u32 = 0x0004;
pub const AFD_POLL_DISCONNECT: u32 = 0x0008;
pub const AFD_POLL_ABORT: u32 = 0x0010;
pub const AFD_POLL_LOCAL_CLOSE: u32 = 0x0020;
pub const AFD_POLL_ACCEPT: u32 = 0x0080;
pub const AFD_POLL_CONNECT_FAIL: u32 = 0x0100;

/// Readable: data or EOF, a connection waiting on a listener, or the
/// connection failing (mio's mapping).
pub const AFD_READ: u32 =
    AFD_POLL_RECEIVE | AFD_POLL_DISCONNECT | AFD_POLL_ACCEPT | AFD_POLL_ABORT | AFD_POLL_CONNECT_FAIL;

/// Writable: send space -- which is also how a connect completing shows --
/// or the connection failing. Windows `select()` reports a failed connect
/// in exceptfds, which is why CPython's selector loop treats exceptfds as
/// writable; CONNECT_FAIL here is the same rule.
pub const AFD_WRITE: u32 = AFD_POLL_SEND | AFD_POLL_ABORT | AFD_POLL_CONNECT_FAIL;

/// The events to poll for a watch. LOCAL_CLOSE is always asked for: it is
/// how a poll learns that the socket it names was closed under it, after
/// which its handle (and possibly its number) no longer names that socket.
#[inline]
pub fn afd_poll_events(read: bool, write: bool) -> u32 {
    let mut ev = AFD_POLL_LOCAL_CLOSE;
    if read {
        ev |= AFD_READ;
    }
    if write {
        ev |= AFD_WRITE;
    }
    ev
}

/// The events a completed poll reports. AFD writes its answer over the
/// request (METHOD_BUFFERED: one buffer serves both), so when it reports no
/// handle, `handles[0]` still holds the events that were ASKED for --
/// LOCAL_CLOSE among them, which read as an answer would end a live watch.
/// Only a reported handle's events are real; mio and wepoll check the same.
#[inline]
pub fn afd_reported_events(number_of_handles: u32, events: u32) -> u32 {
    if number_of_handles == 0 {
        0
    } else {
        events
    }
}

/// What one completed AFD poll means for its watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollOutcome {
    /// Report readiness (either side may be false) and re-arm.
    Ready { readable: bool, writable: bool },
    /// Cancelled: the interest changed. Re-arm if the watch still wants it.
    Cancelled,
    /// The poll could not run (bad handle, ...): report every watched side
    /// once so the callback's own syscall surfaces the error, and stop
    /// polling -- re-arming a poll AFD refuses would spin.
    Failed,
    /// The socket was closed under the watch: report nothing, poll no more.
    Closed,
}

pub fn afd_outcome(status: i32, events: u32, read: bool, write: bool) -> PollOutcome {
    if status == STATUS_CANCELLED {
        PollOutcome::Cancelled
    } else if !nt_success(status) {
        PollOutcome::Failed
    } else if events & AFD_POLL_LOCAL_CLOSE != 0 {
        PollOutcome::Closed
    } else {
        PollOutcome::Ready {
            readable: read && events & AFD_READ != 0,
            writable: write && events & AFD_WRITE != 0,
        }
    }
}

// ---- slot quarantine (W1 defence) --------------------------------------- //

/// Slots retired WITHOUT a port completion -- inline successes under
/// FILE_SKIP_COMPLETION_PORT_ON_SUCCESS, synchronous failures -- held off
/// the freelist until the port has been drained once since.
///
/// Recycling them at once is what turns any packet the kernel queued
/// anyway (skip mode believed set but not, a provider that posts
/// regardless, a warning status) into a use-after-free: the freelist is
/// LIFO, so the next post reuses the slot, and the stray packet then
/// "completes" that live op -- whose buffers are released while the kernel
/// still owns them. Held, the slot is still Completed when such a packet
/// arrives, so it is recognised, counted and dropped instead.
///
/// A packet for a synchronous completion is queued before the issuing call
/// returns, so once a dequeue comes back short of a full batch, every id
/// held before that dequeue started has had its packet (if any) seen.
#[derive(Debug, Default)]
pub struct Quarantine<T> {
    held: Vec<T>,
}

impl<T: Copy> Quarantine<T> {
    pub fn new() -> Self {
        Quarantine { held: Vec::new() }
    }

    pub fn hold(&mut self, id: T) {
        self.held.push(id);
    }

    pub fn len(&self) -> usize {
        self.held.len()
    }

    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    /// Take before a dequeue; pass to `drained` after it.
    pub fn mark(&self) -> usize {
        self.held.len()
    }

    /// The ids now safe to recycle, given a dequeue that started with
    /// `mark` ids held and returned `dequeued` of a possible `batch`.
    pub fn drained(&mut self, mark: usize, dequeued: usize, batch: usize) -> std::vec::Drain<'_, T> {
        let n = if dequeued < batch { mark.min(self.held.len()) } else { 0 };
        self.held.drain(..n)
    }

    /// Everything, unconditionally (teardown, once nothing can arrive).
    pub fn drain_all(&mut self) -> std::vec::Drain<'_, T> {
        self.held.drain(..)
    }
}

// ---- recycled-socket pool (R-033) --------------------------------------- //

/// Sockets recycled by `DisconnectEx(TF_REUSE_SOCKET)`, keyed by
/// (address family, socket type, protocol): AcceptEx takes an accept socket
/// of the LISTENER's kind, so a single shared pool handed an IPv6 listener
/// an IPv4 socket and the accept failed.
#[derive(Debug)]
pub struct SocketPool<S> {
    pools: Vec<((i32, i32, i32), Vec<S>)>,
    len: usize,
    cap: usize,
}

impl<S> SocketPool<S> {
    pub fn new(cap: usize) -> Self {
        SocketPool { pools: Vec::new(), len: 0, cap }
    }

    /// Pool `s`; hands it back when the pool is full (the caller closes it).
    pub fn push(&mut self, key: (i32, i32, i32), s: S) -> Option<S> {
        if self.len >= self.cap {
            return Some(s);
        }
        match self.pools.iter_mut().find(|(k, _)| *k == key) {
            Some((_, v)) => v.push(s),
            None => self.pools.push((key, vec![s])),
        }
        self.len += 1;
        None
    }

    pub fn pop(&mut self, key: (i32, i32, i32)) -> Option<S> {
        let (_, v) = self.pools.iter_mut().find(|(k, _)| *k == key)?;
        let s = v.pop()?;
        self.len -= 1;
        Some(s)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Every pooled socket (teardown).
    pub fn drain_all(&mut self) -> impl Iterator<Item = S> + '_ {
        self.len = 0;
        self.pools.drain(..).flat_map(|(_, v)| v)
    }
}

// ---- retry-on-failure lazy init ----------------------------------------- //

/// `OnceLock::get_or_init` for a fallible initialiser that caches only
/// success. Caching the `io::Result` itself made one transient failure (a
/// probe socket refused at a descriptor limit, say) permanent for the
/// whole process.
pub fn get_or_try_init<T>(cell: &OnceLock<T>, init: impl FnOnce() -> io::Result<T>) -> io::Result<&T> {
    if let Some(v) = cell.get() {
        return Ok(v);
    }
    let v = init()?;
    // A racing thread may have won; either value is as good.
    Ok(cell.get_or_init(|| v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_classes() {
        assert!(nt_success(STATUS_SUCCESS));
        assert!(nt_success(STATUS_PENDING));
        assert!(!nt_success(STATUS_BUFFER_OVERFLOW), "a warning is not success");
        assert!(!nt_error(STATUS_BUFFER_OVERFLOW), "...and not an error: its packet is still queued");
        assert!(nt_error(STATUS_CANCELLED));
        assert!(nt_error(STATUS_PIPE_BROKEN));
        assert!(!nt_error(STATUS_PENDING));
        // Internal is a ULONG_PTR; only its low half is the status.
        assert_eq!(overlapped_status(0xC000_0120), STATUS_CANCELLED);
        assert_eq!(overlapped_status(0xFFFF_FFFF_0000_0000_u64 as usize), STATUS_SUCCESS);
    }

    #[test]
    fn socket_statuses_map_to_what_upper_layers_test() {
        // is_cancelled_error: 995, exactly what WSAGetOverlappedResult
        // reports for CancelIoEx and closesocket aborts.
        assert_eq!(ntstatus_to_wsa(STATUS_CANCELLED), Some(995));
        assert_eq!(ntstatus_to_wsa(0xC000_0240_u32 as i32), Some(995));
        // is_transient_accept_error: resets and aborts.
        assert_eq!(ntstatus_to_wsa(0xC000_020D_u32 as i32), Some(10054)); // CONNECTION_RESET
        assert_eq!(ntstatus_to_wsa(0xC000_013C_u32 as i32), Some(10054)); // REMOTE_DISCONNECT
        assert_eq!(ntstatus_to_wsa(0xC000_0241_u32 as i32), Some(10053)); // CONNECTION_ABORTED
        assert_eq!(ntstatus_to_wsa(0xC000_013B_u32 as i32), Some(10053)); // LOCAL_DISCONNECT

        // The ones a Python caller sees most.
        assert_eq!(ntstatus_to_wsa(0xC000_0236_u32 as i32), Some(10061)); // CONNECTION_REFUSED
        assert_eq!(ntstatus_to_wsa(0xC000_00B5_u32 as i32), Some(10060)); // IO_TIMEOUT
        assert_eq!(ntstatus_to_wsa(0xC000_023D_u32 as i32), Some(10065)); // HOST_UNREACHABLE
        assert_eq!(ntstatus_to_wsa(0xC000_023C_u32 as i32), Some(10051)); // NETWORK_UNREACHABLE
        assert_eq!(ntstatus_to_wsa(0xC000_023F_u32 as i32), Some(10054)); // PORT_UNREACHABLE (UDP)
        assert_eq!(ntstatus_to_wsa(STATUS_BUFFER_OVERFLOW), Some(10040)); // truncated datagram
        assert_eq!(ntstatus_to_wsa(0xC000_009A_u32 as i32), Some(10055)); // INSUFFICIENT_RESOURCES
        assert_eq!(ntstatus_to_wsa(0xC000_0008_u32 as i32), Some(10038)); // INVALID_HANDLE

        // Not in the table: the caller falls back to RtlNtStatusToDosError.
        assert_eq!(ntstatus_to_wsa(STATUS_PIPE_BROKEN), None);
        assert_eq!(ntstatus_to_wsa(0xC000_FFFF_u32 as i32), None);
    }

    #[test]
    fn afd_events_cover_what_select_reports() {
        let r = afd_poll_events(true, false);
        assert!(r & AFD_POLL_ACCEPT != 0, "a listener's readability is a pending connection");
        assert!(r & AFD_POLL_LOCAL_CLOSE != 0);
        assert!(r & AFD_POLL_SEND == 0);
        let w = afd_poll_events(false, true);
        assert!(w & AFD_POLL_SEND != 0);
        assert!(w & AFD_POLL_CONNECT_FAIL != 0, "a failed connect wakes a writer, as on select()");
        assert!(w & AFD_POLL_RECEIVE == 0, "a writer must not be woken by inbound data");
        assert_eq!(afd_poll_events(false, false), AFD_POLL_LOCAL_CLOSE);
    }

    #[test]
    fn afd_outcomes() {
        use PollOutcome::*;
        // Data on a socket watched both ways: readable only.
        assert_eq!(afd_outcome(0, AFD_POLL_RECEIVE, true, true), Ready { readable: true, writable: false });
        // A listener with a pending connection.
        assert_eq!(afd_outcome(0, AFD_POLL_ACCEPT, true, false), Ready { readable: true, writable: false });
        // A connect that failed: both watched sides wake.
        assert_eq!(
            afd_outcome(0, AFD_POLL_CONNECT_FAIL, true, true),
            Ready { readable: true, writable: true }
        );
        // Readiness for a side nobody watches is filtered out.
        assert_eq!(afd_outcome(0, AFD_POLL_SEND, true, false), Ready { readable: false, writable: false });
        assert_eq!(afd_outcome(0, AFD_POLL_LOCAL_CLOSE | AFD_POLL_RECEIVE, true, false), Closed);
        assert_eq!(afd_outcome(STATUS_CANCELLED, 0, true, false), Cancelled);
        assert_eq!(afd_outcome(0xC000_0008_u32 as i32, 0, true, false), Failed);
    }

    #[test]
    fn a_poll_reporting_no_handle_reports_no_events() {
        // The answer overwrites the request: with no handle reported, the
        // events field is still the request's, LOCAL_CLOSE included.
        let asked = afd_poll_events(true, false);
        assert_eq!(afd_reported_events(0, asked), 0);
        assert_eq!(
            afd_outcome(STATUS_SUCCESS, afd_reported_events(0, asked), true, false),
            PollOutcome::Ready { readable: false, writable: false },
            "an empty answer must not read as the socket being closed"
        );
        assert_eq!(afd_reported_events(1, AFD_POLL_RECEIVE), AFD_POLL_RECEIVE);
    }

    #[test]
    fn quarantine_releases_only_after_a_short_dequeue() {
        let mut q: Quarantine<u32> = Quarantine::new();
        q.hold(1);
        q.hold(2);
        let mark = q.mark();
        // A full batch: the port may hold more, possibly a packet for 1 or 2.
        assert_eq!(q.drained(mark, 256, 256).count(), 0);
        assert_eq!(q.len(), 2);
        // Held after the mark: its packet may not have been dequeued yet.
        q.hold(3);
        let released: Vec<u32> = q.drained(mark, 10, 256).collect();
        assert_eq!(released, vec![1, 2]);
        assert_eq!(q.len(), 1, "3 was held after the dequeue began");
        let mark = q.mark();
        assert_eq!(q.drained(mark, 0, 256).collect::<Vec<_>>(), vec![3]);
        assert!(q.is_empty());
    }

    #[test]
    fn socket_pool_is_keyed_by_kind_and_capped() {
        const V4: (i32, i32, i32) = (2, 1, 6);
        const V6: (i32, i32, i32) = (23, 1, 6);
        let mut p: SocketPool<usize> = SocketPool::new(2);
        assert_eq!(p.push(V4, 10), None);
        assert_eq!(p.pop(V6), None, "an IPv4 socket must never reach an IPv6 accept");
        assert_eq!(p.push(V6, 20), None);
        assert_eq!(p.push(V4, 11), Some(11), "over the cap the caller gets it back to close");
        assert_eq!(p.len(), 2);
        assert_eq!(p.pop(V4), Some(10));
        assert_eq!(p.pop(V4), None);
        assert_eq!(p.drain_all().collect::<Vec<_>>(), vec![20]);
        assert!(p.is_empty());
    }

    #[test]
    fn a_failed_init_is_retried_not_cached() {
        let cell: OnceLock<u32> = OnceLock::new();
        let err = get_or_try_init(&cell, || Err(io::Error::other("transient"))).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(cell.get().is_none(), "the failure must not stick");
        assert_eq!(*get_or_try_init(&cell, || Ok(7)).unwrap(), 7);
        // Once set, the initialiser never runs again.
        assert_eq!(*get_or_try_init(&cell, || panic!("re-ran")).unwrap(), 7);
    }
}
