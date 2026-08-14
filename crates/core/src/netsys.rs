//! Cross-platform raw-socket helpers for the transport layer (L2).
//!
//! Thin, allocation-free wrappers over Winsock/libc used by pyshim's
//! transports: socket creation, bind/listen, sockaddr build/parse, option
//! setting, close. Both platforms present the same `RawSocket` (usize)
//! currency as the backends.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::backend::RawSocket;

pub const AF_INET: i32 = 2;
#[cfg(windows)]
pub const AF_INET6: i32 = 23;
#[cfg(not(windows))]
pub const AF_INET6: i32 = 10;

/// Serialized sockaddr storage: bytes valid for connect/bind on the
/// native platform.
pub struct SockAddrBuf {
    pub buf: [u8; 128],
    pub len: usize,
    pub family: i32,
}

pub fn build_sockaddr(addr: SocketAddr) -> SockAddrBuf {
    let mut out = SockAddrBuf { buf: [0u8; 128], len: 0, family: 0 };
    match addr {
        SocketAddr::V4(v4) => {
            out.family = AF_INET;
            // sockaddr_in layout is identical on Windows and Linux:
            // u16 family, u16 port(BE), u32 addr(BE), 8 pad.
            out.buf[0..2].copy_from_slice(&(AF_INET as u16).to_ne_bytes());
            out.buf[2..4].copy_from_slice(&v4.port().to_be_bytes());
            out.buf[4..8].copy_from_slice(&v4.ip().octets());
            out.len = 16;
        }
        SocketAddr::V6(v6) => {
            out.family = AF_INET6;
            // sockaddr_in6: u16 family, u16 port(BE), u32 flowinfo,
            // 16 addr, u32 scope_id — same layout both platforms.
            out.buf[0..2].copy_from_slice(&(AF_INET6 as u16).to_ne_bytes());
            out.buf[2..4].copy_from_slice(&v6.port().to_be_bytes());
            out.buf[4..8].copy_from_slice(&v6.flowinfo().to_ne_bytes());
            out.buf[8..24].copy_from_slice(&v6.ip().octets());
            out.buf[24..28].copy_from_slice(&v6.scope_id().to_ne_bytes());
            out.len = 28;
        }
    }
    out
}

/// A socket address in the forms this engine has to carry.
///
/// `SocketAddr` cannot represent an AF_UNIX path, so a Unix connection's
/// peer and local addresses were simply dropped -- `get_extra_info`
/// returned None on a live `create_unix_connection` /
/// `create_unix_server` transport, leaving no way to learn the peer or
/// the socket path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Addr {
    Inet(SocketAddr),
    /// Filesystem path, or a leading NUL for the Linux abstract namespace.
    /// Empty for an unnamed socket, which is what asyncio reports too.
    Unix(Vec<u8>),
}

/// AF_UNIX sockaddr -> path bytes. `sun_path` starts at offset 2 and is
/// NUL-terminated for a filesystem socket; in Linux's abstract namespace
/// the FIRST byte is NUL and the rest is significant, so it cannot simply
/// be truncated at the first NUL.
pub fn parse_unix_sockaddr(buf: &[u8]) -> Option<Vec<u8>> {
    const AF_UNIX_FAMILY: i32 = 1;
    if buf.len() < 2 || u16::from_ne_bytes([buf[0], buf[1]]) as i32 != AF_UNIX_FAMILY {
        return None;
    }
    let path = &buf[2..];
    if path.first() == Some(&0) {
        let end = path.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
        return Some(path[..end].to_vec());
    }
    let end = path.iter().position(|&b| b == 0).unwrap_or(path.len());
    Some(path[..end].to_vec())
}

pub fn parse_sockaddr(buf: &[u8]) -> Option<SocketAddr> {
    if buf.len() < 8 {
        return None;
    }
    let family = u16::from_ne_bytes([buf[0], buf[1]]) as i32;
    let port = u16::from_be_bytes([buf[2], buf[3]]);
    if family == AF_INET {
        let ip = Ipv4Addr::new(buf[4], buf[5], buf[6], buf[7]);
        Some(SocketAddr::new(IpAddr::V4(ip), port))
    } else if family == AF_INET6 && buf.len() >= 28 {
        let mut octets = [0u8; 16];
        octets.copy_from_slice(&buf[8..24]);
        let flowinfo = u32::from_ne_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let scope = u32::from_ne_bytes([buf[24], buf[25], buf[26], buf[27]]);
        Some(SocketAddr::V6(std::net::SocketAddrV6::new(Ipv6Addr::from(octets), port, flowinfo, scope)))
    } else {
        None
    }
}

#[cfg(not(windows))]
mod imp {
    use super::*;

    fn cvt(rc: i32) -> io::Result<i32> {
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(rc)
        }
    }

    pub fn create_tcp(family: i32) -> io::Result<RawSocket> {
        let fd = cvt(unsafe {
            libc::socket(family, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0)
        })?;
        Ok(fd as RawSocket)
    }

    pub fn bind(sock: RawSocket, sa: &SockAddrBuf) -> io::Result<()> {
        cvt(unsafe { libc::bind(sock as i32, sa.buf.as_ptr().cast(), sa.len as libc::socklen_t) })?;
        Ok(())
    }

    pub fn listen(sock: RawSocket, backlog: i32) -> io::Result<()> {
        cvt(unsafe { libc::listen(sock as i32, backlog) })?;
        Ok(())
    }

    pub fn set_reuse_addr(sock: RawSocket, on: bool) -> io::Result<()> {
        let v: i32 = on as i32;
        cvt(unsafe {
            libc::setsockopt(sock as i32, libc::SOL_SOCKET, libc::SO_REUSEADDR, (&v as *const i32).cast(), 4)
        })?;
        Ok(())
    }

    pub fn set_reuse_port(sock: RawSocket, on: bool) -> io::Result<()> {
        let v: i32 = on as i32;
        cvt(unsafe {
            libc::setsockopt(sock as i32, libc::SOL_SOCKET, libc::SO_REUSEPORT, (&v as *const i32).cast(), 4)
        })?;
        Ok(())
    }

    pub fn set_nodelay(sock: RawSocket, on: bool) -> io::Result<()> {
        let v: i32 = on as i32;
        cvt(unsafe {
            libc::setsockopt(sock as i32, libc::IPPROTO_TCP, libc::TCP_NODELAY, (&v as *const i32).cast(), 4)
        })?;
        Ok(())
    }

    pub fn set_v6only(sock: RawSocket, on: bool) -> io::Result<()> {
        let v: i32 = on as i32;
        cvt(unsafe {
            libc::setsockopt(sock as i32, libc::IPPROTO_IPV6, libc::IPV6_V6ONLY, (&v as *const i32).cast(), 4)
        })?;
        Ok(())
    }

    /// R-038 TCP Fast Open on a listener. The Linux option takes the
    /// pending-SYN-data queue length rather than a boolean.
    pub fn set_fastopen(sock: RawSocket, queue: i32) -> io::Result<()> {
        const TCP_FASTOPEN: i32 = 23;
        cvt(unsafe {
            libc::setsockopt(sock as i32, libc::IPPROTO_TCP, TCP_FASTOPEN, (&queue as *const i32).cast(), 4)
        })?;
        Ok(())
    }

    /// R-038 SIO_LOOPBACK_FAST_PATH has no Linux counterpart.
    pub fn set_loopback_fast_path(_sock: RawSocket) -> io::Result<()> {
        Ok(())
    }

    pub fn shutdown_send(sock: RawSocket) -> io::Result<()> {
        cvt(unsafe { libc::shutdown(sock as i32, libc::SHUT_WR) })?;
        Ok(())
    }

    pub fn close(sock: RawSocket) {
        unsafe { libc::close(sock as i32) };
    }

    fn name_of(
        sock: RawSocket,
        f: unsafe extern "C" fn(i32, *mut libc::sockaddr, *mut libc::socklen_t) -> i32,
    ) -> io::Result<SocketAddr> {
        match any_name_of(sock, f)? {
            Addr::Inet(a) => Ok(a),
            Addr::Unix(_) => Err(io::Error::new(io::ErrorKind::InvalidData, "not an internet address")),
        }
    }

    fn any_name_of(
        sock: RawSocket,
        f: unsafe extern "C" fn(i32, *mut libc::sockaddr, *mut libc::socklen_t) -> i32,
    ) -> io::Result<Addr> {
        let mut buf = [0u8; 128];
        let mut len = buf.len() as libc::socklen_t;
        cvt(unsafe { f(sock as i32, buf.as_mut_ptr().cast(), &mut len) })?;
        let raw = &buf[..len as usize];
        if let Some(a) = parse_sockaddr(raw) {
            return Ok(Addr::Inet(a));
        }
        if let Some(p) = parse_unix_sockaddr(raw) {
            return Ok(Addr::Unix(p));
        }
        Err(io::Error::new(io::ErrorKind::InvalidData, "unparseable sockaddr"))
    }

    pub fn peername_any(sock: RawSocket) -> io::Result<Addr> {
        any_name_of(sock, libc::getpeername)
    }

    pub fn sockname_any(sock: RawSocket) -> io::Result<Addr> {
        any_name_of(sock, libc::getsockname)
    }

    pub fn peername(sock: RawSocket) -> io::Result<SocketAddr> {
        name_of(sock, libc::getpeername)
    }

    pub fn sockname(sock: RawSocket) -> io::Result<SocketAddr> {
        name_of(sock, libc::getsockname)
    }
}

#[cfg(windows)]
mod imp {
    use super::*;
    use windows_sys::Win32::Foundation::{SetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT};
    use windows_sys::Win32::Networking::WinSock::{
        bind as ws_bind, closesocket, getpeername, getsockname, listen as ws_listen, setsockopt, shutdown,
        WSAGetLastError, WSAIoctl, WSASocketW, IPPROTO_TCP, SD_SEND, SOCKADDR, SOCKET_ERROR, SOL_SOCKET,
        SO_REUSEADDR, TCP_NODELAY, WSAEINVAL, WSA_FLAG_OVERLAPPED,
    };

    fn wsa_err() -> io::Error {
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    /// Not in windows-sys 0.59's WinSock bindings; from winsock2.h.
    pub const WSA_FLAG_NO_HANDLE_INHERIT: u32 = 0x80;

    /// Clear a socket's inheritable bit after the fact.
    ///
    /// The pre-Windows-8 fallback for `WSA_FLAG_NO_HANDLE_INHERIT`: it
    /// leaves a window in which a concurrent CreateProcess could still
    /// capture the handle, which is exactly why the flag is preferred.
    pub fn clear_handle_inherit(sock: RawSocket) {
        unsafe { SetHandleInformation(sock as HANDLE, HANDLE_FLAG_INHERIT, 0) };
    }

    pub fn create_tcp(family: i32) -> io::Result<RawSocket> {
        crate::backend::iocp::ensure_winsock();
        // Sockets were created inheritable. A child started with handle
        // inheritance enabled -- any subprocess the application spawns
        // with `close_fds=False`-equivalent semantics -- therefore
        // captured the listener and every live connection, keeping the
        // port bound and holding peers' connections open long after the
        // server closed its own handles.
        let flags = WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT;
        let s = unsafe { WSASocketW(family, 1, IPPROTO_TCP, std::ptr::null(), 0, flags) };
        if s != !0usize {
            return Ok(s);
        }
        // WSAEINVAL here means the flag itself was refused (pre-Win8, or
        // a layered provider that does not implement it) -- not that the
        // socket could not be made. Retry without it and clear the bit.
        if unsafe { WSAGetLastError() } != WSAEINVAL {
            return Err(wsa_err());
        }
        let s = unsafe { WSASocketW(family, 1, IPPROTO_TCP, std::ptr::null(), 0, WSA_FLAG_OVERLAPPED) };
        if s == !0usize {
            return Err(wsa_err());
        }
        clear_handle_inherit(s);
        Ok(s)
    }

    pub fn bind(sock: RawSocket, sa: &SockAddrBuf) -> io::Result<()> {
        let rc = unsafe { ws_bind(sock, sa.buf.as_ptr().cast::<SOCKADDR>(), sa.len as i32) };
        if rc == SOCKET_ERROR {
            return Err(wsa_err());
        }
        Ok(())
    }

    pub fn listen(sock: RawSocket, backlog: i32) -> io::Result<()> {
        let rc = unsafe { ws_listen(sock, backlog) };
        if rc == SOCKET_ERROR {
            return Err(wsa_err());
        }
        Ok(())
    }

    pub fn set_reuse_addr(sock: RawSocket, on: bool) -> io::Result<()> {
        let v: u32 = on as u32;
        let rc = unsafe { setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (&v as *const u32).cast(), 4) };
        if rc == SOCKET_ERROR {
            return Err(wsa_err());
        }
        Ok(())
    }

    pub fn set_reuse_port(_sock: RawSocket, _on: bool) -> io::Result<()> {
        // No SO_REUSEPORT on Windows; load distribution comes from
        // WSADuplicateSocketW listener sharing (R-090).
        Err(io::Error::new(io::ErrorKind::Unsupported, "SO_REUSEPORT is not available on Windows"))
    }

    pub fn set_v6only(sock: RawSocket, on: bool) -> io::Result<()> {
        // IPPROTO_IPV6 = 41, IPV6_V6ONLY = 27 (ws2ipdef.h). Windows
        // defaults this ON, so setting it is belt-and-braces there --
        // it is Linux, which defaults it off, that needs it.
        let v: u32 = on as u32;
        let rc = unsafe { setsockopt(sock, 41, 27, (&v as *const u32).cast(), 4) };
        if rc == SOCKET_ERROR {
            return Err(wsa_err());
        }
        Ok(())
    }

    /// R-038 TCP Fast Open on a listener. Windows takes a boolean here,
    /// unlike Linux's queue length.
    pub fn set_fastopen(sock: RawSocket, _queue: i32) -> io::Result<()> {
        const TCP_FASTOPEN: i32 = 15;
        let v: u32 = 1;
        let rc = unsafe { setsockopt(sock, IPPROTO_TCP, TCP_FASTOPEN, (&v as *const u32).cast(), 4) };
        if rc == SOCKET_ERROR {
            return Err(wsa_err());
        }
        Ok(())
    }

    /// R-038 SIO_LOOPBACK_FAST_PATH. Best-effort: it needs both ends to
    /// set it and is unsupported on some builds, so a failure is not a
    /// reason to fail the connection.
    pub fn set_loopback_fast_path(sock: RawSocket) -> io::Result<()> {
        const SIO_LOOPBACK_FAST_PATH: u32 = 0x9800_0010;
        let mut enabled: u32 = 1;
        let mut bytes: u32 = 0;
        unsafe {
            let _ = WSAIoctl(
                sock,
                SIO_LOOPBACK_FAST_PATH,
                (&mut enabled as *mut u32).cast(),
                4,
                std::ptr::null_mut(),
                0,
                &mut bytes,
                std::ptr::null_mut(),
                None,
            );
        }
        Ok(())
    }

    pub fn set_nodelay(sock: RawSocket, on: bool) -> io::Result<()> {
        let v: u32 = on as u32;
        let rc = unsafe { setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (&v as *const u32).cast(), 4) };
        if rc == SOCKET_ERROR {
            return Err(wsa_err());
        }
        Ok(())
    }

    pub fn shutdown_send(sock: RawSocket) -> io::Result<()> {
        let rc = unsafe { shutdown(sock, SD_SEND) };
        if rc == SOCKET_ERROR {
            return Err(wsa_err());
        }
        Ok(())
    }

    pub fn close(sock: RawSocket) {
        unsafe { closesocket(sock) };
    }

    /// AF_UNIX transports are POSIX-only here (R-057), so on Windows the
    /// "any" forms are just the Internet ones.
    pub fn peername_any(sock: RawSocket) -> io::Result<Addr> {
        peername(sock).map(Addr::Inet)
    }

    pub fn sockname_any(sock: RawSocket) -> io::Result<Addr> {
        sockname(sock).map(Addr::Inet)
    }

    pub fn peername(sock: RawSocket) -> io::Result<SocketAddr> {
        let mut buf = [0u8; 128];
        let mut len = buf.len() as i32;
        let rc = unsafe { getpeername(sock, buf.as_mut_ptr().cast::<SOCKADDR>(), &mut len) };
        if rc == SOCKET_ERROR {
            return Err(wsa_err());
        }
        parse_sockaddr(&buf[..len as usize])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unparseable sockaddr"))
    }

    pub fn sockname(sock: RawSocket) -> io::Result<SocketAddr> {
        let mut buf = [0u8; 128];
        let mut len = buf.len() as i32;
        let rc = unsafe { getsockname(sock, buf.as_mut_ptr().cast::<SOCKADDR>(), &mut len) };
        if rc == SOCKET_ERROR {
            return Err(wsa_err());
        }
        parse_sockaddr(&buf[..len as usize])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unparseable sockaddr"))
    }
}

pub use imp::{
    bind, close, create_tcp, listen, peername, peername_any, set_fastopen, set_loopback_fast_path,
    set_nodelay, set_reuse_addr, set_reuse_port, set_v6only, shutdown_send, sockname, sockname_any,
};

/// Windows-only: the IOCP/RIO backends create accept sockets themselves,
/// so they need the same no-inherit handling `create_tcp` applies.
#[cfg(windows)]
pub use imp::{clear_handle_inherit, WSA_FLAG_NO_HANDLE_INHERIT};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sockaddr_roundtrip_v4() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let sa = build_sockaddr(addr);
        assert_eq!(parse_sockaddr(&sa.buf[..sa.len]), Some(addr));
    }

    #[test]
    fn sockaddr_roundtrip_v6() {
        let addr: SocketAddr = "[::1]:443".parse().unwrap();
        let sa = build_sockaddr(addr);
        assert_eq!(parse_sockaddr(&sa.buf[..sa.len]), Some(addr));
    }

    #[test]
    fn create_bind_listen_names() {
        let s = create_tcp(AF_INET).unwrap();
        set_reuse_addr(s, true).unwrap();
        bind(s, &build_sockaddr("127.0.0.1:0".parse().unwrap())).unwrap();
        listen(s, 16).unwrap();
        let name = sockname(s).unwrap();
        assert_eq!(name.ip().to_string(), "127.0.0.1");
        assert_ne!(name.port(), 0);
        close(s);
    }
}
