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
        let mut buf = [0u8; 128];
        let mut len = buf.len() as libc::socklen_t;
        cvt(unsafe { f(sock as i32, buf.as_mut_ptr().cast(), &mut len) })?;
        parse_sockaddr(&buf[..len as usize])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unparseable sockaddr"))
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
    use windows_sys::Win32::Networking::WinSock::{
        bind as ws_bind, closesocket, getpeername, getsockname, listen as ws_listen, setsockopt, shutdown,
        WSAGetLastError, WSASocketW, IPPROTO_TCP, SD_SEND, SOCKADDR, SOCKET_ERROR, SOL_SOCKET, SO_REUSEADDR,
        TCP_NODELAY, WSA_FLAG_OVERLAPPED,
    };

    fn wsa_err() -> io::Error {
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    pub fn create_tcp(family: i32) -> io::Result<RawSocket> {
        crate::backend::iocp::ensure_winsock();
        let s = unsafe { WSASocketW(family, 1, IPPROTO_TCP, std::ptr::null(), 0, WSA_FLAG_OVERLAPPED) };
        if s == !0usize {
            return Err(wsa_err());
        }
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
    bind, close, create_tcp, listen, peername, set_nodelay, set_reuse_addr, set_reuse_port, shutdown_send,
    sockname,
};

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
