//! RIO diagnosis probe (run on Windows: `cargo run --release --example rio_probe`).
//!
//! Runs 4-5 on hardware (Windows 11 build 26200, beta channel) showed a
//! fully valid function table (bytes=112, all pointers resolved) while
//! EVERY `RIOCreateCompletionQueue` variant — null, event, and IOCP
//! notification, with or without a live REGISTERED_IO socket — failed
//! with WSA 10014. The same run also showed a STALE last-error surviving
//! a successful call (`iocp port: ... (err 6)`), so the 10014 itself may
//! be detritus and the real failure reason invisible.
//!
//! This probe therefore establishes, in order:
//!   1. WHERE the function pointers point (mswsock.dll, or an AV/LSP
//!      hook module) — GetModuleHandleEx(FROM_ADDRESS).
//!   2. WHETHER the Winsock catalog is layered (LSP chains break RIO).
//!   3. WHAT each RIO call actually reports, with SetLastError(0) called
//!      immediately before it — err 0 after a failure means the call
//!      failed without setting an error at all.
//!   4. Whether RIORegisterBuffer (no CQ involved) works — separates
//!      per-process RIO init from CQ creation specifically.
//!   5. CQ creation under every notification variant and several sizes,
//!      with and without a live/bound/listening RIO socket.
//!
//! Send the full output back.

#[cfg(not(windows))]
fn main() {
    println!("rio_probe is Windows-only");
}

#[cfg(windows)]
fn main() {
    win::main();
}

#[cfg(windows)]
mod win {
    use std::mem::{size_of, zeroed};

    use windows_sys::core::GUID;
    use windows_sys::Win32::Foundation::{
        GetLastError, SetLastError, HANDLE, HMODULE, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::Networking::WinSock::{
        bind, closesocket, listen, WSAEnumProtocolsW, WSAGetLastError, WSAIoctl, WSASocketW,
        WSAStartup, AF_INET, IPPROTO_TCP, RIO_BUFFERID, RIO_CQ, RIO_EVENT_COMPLETION,
        RIO_EXTENSION_FUNCTION_TABLE, RIO_IOCP_COMPLETION, RIO_NOTIFICATION_COMPLETION,
        RIO_NOTIFICATION_COMPLETION_0, RIO_NOTIFICATION_COMPLETION_0_0,
        RIO_NOTIFICATION_COMPLETION_0_1, SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER, SOCKADDR,
        SOCKADDR_IN, SOCKET, SOCKET_ERROR, WSADATA, WSAID_MULTIPLE_RIO, WSAPROTOCOL_INFOW,
        WSA_FLAG_OVERLAPPED, WSA_FLAG_REGISTERED_IO,
    };
    use windows_sys::Win32::System::LibraryLoader::{
        GetModuleFileNameW, GetModuleHandleExW, GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS,
        GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
    };
    use windows_sys::Win32::System::SystemInformation::{GetNativeSystemInfo, SYSTEM_INFO};
    use windows_sys::Win32::System::Threading::CreateEventW;
    use windows_sys::Win32::System::IO::{CreateIoCompletionPort, OVERLAPPED};

    fn last() -> i32 {
        unsafe { WSAGetLastError() }
    }

    /// WSAGetLastError with the stale-error trap closed: 0 after a failed
    /// call means the callee never set an error.
    fn clear_err() {
        unsafe { SetLastError(0) };
    }

    fn module_of(addr: usize) -> String {
        unsafe {
            let mut hmod: HMODULE = std::ptr::null_mut();
            let ok = GetModuleHandleExW(
                GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS
                    | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                addr as *const u16,
                &mut hmod,
            );
            if ok == 0 {
                return format!("<no module for {addr:#x}: err {}>", GetLastError());
            }
            let mut buf = [0u16; 300];
            let n = GetModuleFileNameW(hmod, buf.as_mut_ptr(), 300);
            String::from_utf16_lossy(&buf[..n as usize])
        }
    }

    fn dump_catalog() {
        println!("== winsock catalog (ChainLen > 1 means an LSP is layered) ==");
        unsafe {
            let mut len: u32 = 0;
            WSAEnumProtocolsW(std::ptr::null(), std::ptr::null_mut(), &mut len);
            let n = (len as usize / size_of::<WSAPROTOCOL_INFOW>()) + 2;
            let mut buf: Vec<WSAPROTOCOL_INFOW> = vec![zeroed(); n];
            let rc = WSAEnumProtocolsW(std::ptr::null(), buf.as_mut_ptr(), &mut len);
            if rc == SOCKET_ERROR {
                println!("  WSAEnumProtocolsW FAILED: WSA {}", last());
                return;
            }
            for p in buf.iter().take(rc as usize) {
                let name_end = p.szProtocol.iter().position(|&c| c == 0).unwrap_or(0);
                let name = String::from_utf16_lossy(&p.szProtocol[..name_end]);
                println!(
                    "  [{}] chain_len={} flags={:#x} {}",
                    p.dwCatalogEntryId, p.ProtocolChain.ChainLen, p.dwServiceFlags1, name
                );
            }
        }
    }

    fn table(with_rio_flag: bool) -> Option<RIO_EXTENSION_FUNCTION_TABLE> {
        unsafe {
            let flags = if with_rio_flag {
                WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO
            } else {
                WSA_FLAG_OVERLAPPED
            };
            let probe = WSASocketW(AF_INET as i32, 1, IPPROTO_TCP, std::ptr::null(), 0, flags);
            if probe == !0usize {
                println!("  probe socket (rio_flag={with_rio_flag}) FAILED: WSA {}", last());
                return None;
            }
            let guid: GUID = WSAID_MULTIPLE_RIO;
            let mut t: RIO_EXTENSION_FUNCTION_TABLE = zeroed();
            t.cbSize = size_of::<RIO_EXTENSION_FUNCTION_TABLE>() as u32;
            let mut bytes: u32 = 0;
            let rc = WSAIoctl(
                probe,
                SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
                (&guid as *const GUID).cast(),
                size_of::<GUID>() as u32,
                (&mut t as *mut RIO_EXTENSION_FUNCTION_TABLE).cast(),
                size_of::<RIO_EXTENSION_FUNCTION_TABLE>() as u32,
                &mut bytes,
                std::ptr::null_mut(),
                None,
            );
            closesocket(probe);
            if rc == SOCKET_ERROR {
                println!("  table ioctl (rio_flag={with_rio_flag}) FAILED: WSA {}", last());
                return None;
            }
            println!(
                "  table ioctl (rio_flag={with_rio_flag}) OK: bytes={bytes} cbSize={} (struct size {})",
                t.cbSize,
                size_of::<RIO_EXTENSION_FUNCTION_TABLE>()
            );
            Some(t)
        }
    }

    fn try_cq(
        t: &RIO_EXTENSION_FUNCTION_TABLE,
        label: &str,
        size: u32,
        notify: *const RIO_NOTIFICATION_COMPLETION,
    ) -> RIO_CQ {
        clear_err();
        let cq = unsafe { (t.RIOCreateCompletionQueue.unwrap())(size, notify) };
        if cq == 0 {
            let e = last();
            println!(
                "  CQ[{label} size={size}] FAILED: WSA {e}{}",
                if e == 0 { "  <- failed WITHOUT setting an error" } else { "" }
            );
        } else {
            println!("  CQ[{label} size={size}] OK: handle {cq:#x}");
        }
        cq
    }

    fn try_register_buffer(t: &RIO_EXTENSION_FUNCTION_TABLE, label: &str) -> bool {
        let buf = vec![0u8; 65536];
        clear_err();
        let id: RIO_BUFFERID = unsafe { (t.RIORegisterBuffer.unwrap())(buf.as_ptr(), 65536) };
        // Header sentinel is (RIO_BUFFERID)0xFFFFFFFF: ZERO-extended on
        // x64, not -1 (run-6 log printed a failure as "OK: id 0xffffffff").
        if id == 0xFFFF_FFFF {
            let e = last();
            println!(
                "  RIORegisterBuffer[{label}] FAILED: WSA {e}{}",
                if e == 0 { "  <- failed WITHOUT setting an error" } else { "" }
            );
            false
        } else {
            println!("  RIORegisterBuffer[{label}] OK: id {id:#x}");
            unsafe { (t.RIODeregisterBuffer.unwrap())(id) };
            true
        }
    }

    fn rio_socket() -> SOCKET {
        unsafe {
            WSASocketW(
                AF_INET as i32,
                1,
                IPPROTO_TCP,
                std::ptr::null(),
                0,
                WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO,
            )
        }
    }

    pub fn main() {
        unsafe {
            let mut data: WSADATA = zeroed();
            WSAStartup(0x0202, &mut data);
        }

        // 9 = x64, 12 = ARM64. An x64 binary reporting a native ARM64
        // system is running under emulation — a strong RIO-breakage
        // suspect (the ring registration path is not emulation-friendly).
        let mut si: SYSTEM_INFO = unsafe { zeroed() };
        unsafe { GetNativeSystemInfo(&mut si) };
        let arch = unsafe { si.Anonymous.Anonymous.wProcessorArchitecture };
        println!(
            "== native machine == arch={arch} (9=x64, 12=ARM64; this probe is an x64 binary{})",
            if arch == 12 { " -- RUNNING EMULATED" } else { "" }
        );

        println!("== function table ==");
        let _ = table(false);
        let Some(t) = table(true) else {
            println!("no table; stopping");
            return;
        };
        let fns: [(&str, usize); 6] = [
            ("RIOCreateCQ", t.RIOCreateCompletionQueue.map_or(0, |f| f as usize)),
            ("RIOCreateRQ", t.RIOCreateRequestQueue.map_or(0, |f| f as usize)),
            ("RIOReceive", t.RIOReceive.map_or(0, |f| f as usize)),
            ("RIOSend", t.RIOSend.map_or(0, |f| f as usize)),
            ("RIORegister", t.RIORegisterBuffer.map_or(0, |f| f as usize)),
            ("RIONotify", t.RIONotify.map_or(0, |f| f as usize)),
        ];
        println!("== module resolution (should all be mswsock.dll) ==");
        for (n, p) in fns {
            println!("  {n:12} = {p:#x}  {}", module_of(p));
        }

        dump_catalog();

        println!("== RIORegisterBuffer before any CQ (per-process init vs CQ-specific) ==");
        try_register_buffer(&t, "no-rio-socket");
        let anchor = rio_socket();
        println!("  anchor rio socket: {anchor:#x}");
        try_register_buffer(&t, "rio-socket-open");

        println!("== CQ null-notify: size sweep, anchor open ==");
        for size in [1u32, 16, 1024] {
            let cq = try_cq(&t, "null-notify", size, std::ptr::null());
            if cq != 0 {
                unsafe { (t.RIOCloseCompletionQueue.unwrap())(cq) };
            }
        }

        println!("== CQ null-notify: anchor bound + listening ==");
        unsafe {
            let mut sa: SOCKADDR_IN = zeroed();
            sa.sin_family = AF_INET;
            // port 0, INADDR_ANY
            let rc = bind(anchor, (&sa as *const SOCKADDR_IN).cast::<SOCKADDR>(), size_of::<SOCKADDR_IN>() as i32);
            println!("  bind: rc={rc} (err {})", if rc != 0 { last() } else { 0 });
            let rc = listen(anchor, 16);
            println!("  listen: rc={rc} (err {})", if rc != 0 { last() } else { 0 });
        }
        let cq_poll = try_cq(&t, "null-notify/listening", 1024, std::ptr::null());

        println!("== CQ event / IOCP variants ==");
        let event = unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) };
        let ev_notify = RIO_NOTIFICATION_COMPLETION {
            Type: RIO_EVENT_COMPLETION,
            Anonymous: RIO_NOTIFICATION_COMPLETION_0 {
                Event: RIO_NOTIFICATION_COMPLETION_0_0 { EventHandle: event, NotifyReset: 1 },
            },
        };
        let cq_event = try_cq(&t, "event-notify", 1024, &ev_notify);

        let port =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
        let mut ov: Box<OVERLAPPED> = Box::new(unsafe { zeroed() });
        let iocp_notify = RIO_NOTIFICATION_COMPLETION {
            Type: RIO_IOCP_COMPLETION,
            Anonymous: RIO_NOTIFICATION_COMPLETION_0 {
                Iocp: RIO_NOTIFICATION_COMPLETION_0_1 {
                    IocpHandle: port as HANDLE,
                    CompletionKey: 3usize as *mut core::ffi::c_void,
                    Overlapped: (&mut *ov as *mut OVERLAPPED).cast(),
                },
            },
        };
        let cq_iocp = try_cq(&t, "iocp-notify", 1024, &iocp_notify);

        println!(
            "  struct sizes: RIO_NOTIFICATION_COMPLETION={} union-offset={}",
            size_of::<RIO_NOTIFICATION_COMPLETION>(),
            std::mem::offset_of!(RIO_NOTIFICATION_COMPLETION, Anonymous),
        );

        // RQ + registered buffer on whichever CQ worked.
        let cq = [cq_iocp, cq_event, cq_poll].into_iter().find(|&c| c != 0).unwrap_or(0);
        if cq != 0 {
            println!("== RQ + buffer on working CQ ==");
            unsafe {
                let s = rio_socket();
                println!("  rio socket: {s:#x}");
                clear_err();
                let rq = (t.RIOCreateRequestQueue.unwrap())(s, 32, 1, 32, 1, cq, cq, std::ptr::null());
                if rq == 0 {
                    println!("  RQ FAILED: WSA {}", last());
                } else {
                    println!("  RQ OK: {rq:#x}");
                }
                closesocket(s);
            }
        }
        println!("== done ==");
    }
}
