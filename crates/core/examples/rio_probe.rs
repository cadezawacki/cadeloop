//! RIO diagnosis probe (run on Windows: `cargo run --release --example rio_probe`).
//!
//! The first hardware run failed `RIOCreateCompletionQueue` with
//! WSAEFAULT under IOCP notification. This probe isolates the ingredient:
//! it prints the resolved function table, then tries CQ creation under
//! every notification variant (none / event / IOCP, with permutations),
//! and finally exercises RQ creation + a registered buffer on whichever
//! CQ succeeded. Send the full output back.

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
    use windows_sys::Win32::Foundation::{GetLastError, HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Networking::WinSock::{
        closesocket, WSAGetLastError, WSAIoctl, WSASocketW, WSAStartup, AF_INET, IPPROTO_TCP,
        RIO_BUFFERID, RIO_CQ, RIO_EVENT_COMPLETION, RIO_EXTENSION_FUNCTION_TABLE,
        RIO_IOCP_COMPLETION, RIO_NOTIFICATION_COMPLETION, RIO_NOTIFICATION_COMPLETION_0,
        RIO_NOTIFICATION_COMPLETION_0_0, RIO_NOTIFICATION_COMPLETION_0_1,
        SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER, SOCKET_ERROR, WSADATA, WSAID_MULTIPLE_RIO,
        WSA_FLAG_OVERLAPPED, WSA_FLAG_REGISTERED_IO,
    };
    use windows_sys::Win32::System::Threading::CreateEventW;
    use windows_sys::Win32::System::IO::{CreateIoCompletionPort, OVERLAPPED};

    fn last() -> i32 {
        unsafe { WSAGetLastError() }
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
            let fns: [(&str, usize); 13] = [
                ("RIOReceive", t.RIOReceive.map_or(0, |f| f as usize)),
                ("RIOReceiveEx", t.RIOReceiveEx.map_or(0, |f| f as usize)),
                ("RIOSend", t.RIOSend.map_or(0, |f| f as usize)),
                ("RIOSendEx", t.RIOSendEx.map_or(0, |f| f as usize)),
                ("RIOCloseCQ", t.RIOCloseCompletionQueue.map_or(0, |f| f as usize)),
                ("RIOCreateCQ", t.RIOCreateCompletionQueue.map_or(0, |f| f as usize)),
                ("RIOCreateRQ", t.RIOCreateRequestQueue.map_or(0, |f| f as usize)),
                ("RIODequeue", t.RIODequeueCompletion.map_or(0, |f| f as usize)),
                ("RIODeregister", t.RIODeregisterBuffer.map_or(0, |f| f as usize)),
                ("RIONotify", t.RIONotify.map_or(0, |f| f as usize)),
                ("RIORegister", t.RIORegisterBuffer.map_or(0, |f| f as usize)),
                ("RIOResizeCQ", t.RIOResizeCompletionQueue.map_or(0, |f| f as usize)),
                ("RIOResizeRQ", t.RIOResizeRequestQueue.map_or(0, |f| f as usize)),
            ];
            for (n, p) in fns {
                println!("    {n:14} = {p:#x}");
            }
            Some(t)
        }
    }

    fn try_cq(t: &RIO_EXTENSION_FUNCTION_TABLE, label: &str, notify: *const RIO_NOTIFICATION_COMPLETION) -> RIO_CQ {
        let cq = unsafe { (t.RIOCreateCompletionQueue.unwrap())(1024, notify) };
        if cq == 0 {
            println!("  CQ[{label}] FAILED: WSA {}", last());
        } else {
            println!("  CQ[{label}] OK: handle {cq:#x}");
        }
        cq
    }

    pub fn main() {
        unsafe {
            let mut data: WSADATA = zeroed();
            WSAStartup(0x0202, &mut data);
        }
        println!("== function table ==");
        let _ = table(false);
        let Some(t) = table(true) else {
            println!("no table; stopping");
            return;
        };

        // Run 4 showed EVERY variant below failing WSAEFAULT when no
        // REGISTERED_IO socket was alive (both table sockets were closed
        // by then), with a perfectly valid table. Hypothesis: mswsock's
        // per-process RIO state is torn down with the last RIO socket.
        // Prove it by trying CQ creation in both worlds.
        println!("== CQ variants (no RIO socket alive; run-4 repro, expect WSAEFAULT) ==");
        let cq_dead = try_cq(&t, "null-notify/no-rio-socket", std::ptr::null());
        if cq_dead != 0 {
            unsafe { (t.RIOCloseCompletionQueue.unwrap())(cq_dead) };
        }

        println!("== CQ variants (RIO socket held open) ==");
        let anchor = unsafe {
            WSASocketW(
                AF_INET as i32,
                1,
                IPPROTO_TCP,
                std::ptr::null(),
                0,
                WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO,
            )
        };
        println!("  anchor rio socket: {anchor:#x}");

        // 1. no notification (pure polling)
        let cq_poll = try_cq(&t, "null-notify", std::ptr::null());

        // 2. event notification
        let event = unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) };
        println!("  event handle: {event:?}");
        let ev_notify = RIO_NOTIFICATION_COMPLETION {
            Type: RIO_EVENT_COMPLETION,
            Anonymous: RIO_NOTIFICATION_COMPLETION_0 {
                Event: RIO_NOTIFICATION_COMPLETION_0_0 { EventHandle: event, NotifyReset: 1 },
            },
        };
        let cq_event = try_cq(&t, "event-notify", &ev_notify);

        // 3. IOCP notification, exactly as the backend does it
        let port =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
        println!("  iocp port: {port:?} (err {})", unsafe { GetLastError() });
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
        let cq_iocp = try_cq(&t, "iocp-notify", &iocp_notify);

        // 4. IOCP notification permutations (if 3 failed)
        if cq_iocp == 0 {
            let mut ov2: OVERLAPPED = unsafe { zeroed() };
            let v = RIO_NOTIFICATION_COMPLETION {
                Type: RIO_IOCP_COMPLETION,
                Anonymous: RIO_NOTIFICATION_COMPLETION_0 {
                    Iocp: RIO_NOTIFICATION_COMPLETION_0_1 {
                        IocpHandle: port as HANDLE,
                        CompletionKey: std::ptr::null_mut(),
                        Overlapped: (&mut ov2 as *mut OVERLAPPED).cast(),
                    },
                },
            };
            try_cq(&t, "iocp-notify/key0-stack-ov", &v);
            println!(
                "  struct sizes: RIO_NOTIFICATION_COMPLETION={} Type-offset=0 union-offset={}",
                size_of::<RIO_NOTIFICATION_COMPLETION>(),
                std::mem::offset_of!(RIO_NOTIFICATION_COMPLETION, Anonymous),
            );
        }

        // 5. RQ + registered buffer on whichever CQ works
        let cq = [cq_iocp, cq_event, cq_poll].into_iter().find(|&c| c != 0).unwrap_or(0);
        if cq != 0 {
            println!("== RQ + buffer on working CQ ==");
            unsafe {
                let s = WSASocketW(
                    AF_INET as i32,
                    1,
                    IPPROTO_TCP,
                    std::ptr::null(),
                    0,
                    WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO,
                );
                println!("  rio socket: {s:#x}");
                let rq = (t.RIOCreateRequestQueue.unwrap())(s, 32, 1, 32, 1, cq, cq, std::ptr::null());
                if rq == 0 {
                    println!("  RQ FAILED: WSA {}", last());
                } else {
                    println!("  RQ OK: {rq:#x}");
                }
                let buf = vec![0u8; 65536];
                let id: RIO_BUFFERID = (t.RIORegisterBuffer.unwrap())(buf.as_ptr(), 65536);
                if id == -1 {
                    println!("  RIORegisterBuffer FAILED: WSA {}", last());
                } else {
                    println!("  RIORegisterBuffer OK: id {id:#x}");
                    (t.RIODeregisterBuffer.unwrap())(id);
                }
                closesocket(s);
            }
        }
        println!("== done ==");
    }
}
