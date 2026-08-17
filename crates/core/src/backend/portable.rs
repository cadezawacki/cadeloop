//! Dev/test backend for non-Windows hosts.
//!
//! Provides only what L1 scheduling semantics need: a blocking wait with
//! timeout and a thread-safe wakeup. No socket ops (all return
//! `Unsupported`). This backend exists so M0 conformance (call_soon /
//! timers / threadsafe wakeup) is testable on any dev machine and in Linux
//! CI; shipping wheels are Windows-only and never select it.

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use super::{Completion, IoBackend, IoSlice, RawSocket, Wakeup};
use crate::opslab::OpId;

#[derive(Default)]
struct Shared {
    pending_wakeups: Mutex<usize>,
    condvar: Condvar,
    /// Diagnostic: total wakeups ever posted.
    posted: AtomicUsize,
}

pub struct PortableBackend {
    shared: Arc<Shared>,
}

impl PortableBackend {
    pub fn new() -> Self {
        PortableBackend { shared: Arc::new(Shared::default()) }
    }
}

impl Default for PortableBackend {
    fn default() -> Self {
        Self::new()
    }
}

struct PortableWakeup {
    shared: Arc<Shared>,
}

impl Wakeup for PortableWakeup {
    fn wake(&self) {
        let mut pending = self.shared.pending_wakeups.lock().unwrap();
        *pending += 1;
        self.shared.posted.fetch_add(1, Ordering::Relaxed);
        drop(pending);
        self.shared.condvar.notify_one();
    }
}

fn unsupported(what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!("{what}: socket I/O is not available on the portable dev backend (Windows-only feature)"),
    )
}

impl IoBackend for PortableBackend {
    fn post_accept(&mut self, _listener: RawSocket) -> io::Result<OpId> {
        Err(unsupported("post_accept"))
    }
    fn post_recv(&mut self, _socket: RawSocket, _buf: *mut u8, _len: u32) -> io::Result<OpId> {
        Err(unsupported("post_recv"))
    }
    fn post_send(&mut self, _socket: RawSocket, _bufs: &[IoSlice]) -> io::Result<OpId> {
        Err(unsupported("post_send"))
    }
    fn post_connect(&mut self, _socket: RawSocket, _addr: &[u8]) -> io::Result<OpId> {
        Err(unsupported("post_connect"))
    }
    fn post_disconnect_reuse(&mut self, _socket: RawSocket) -> io::Result<OpId> {
        Err(unsupported("post_disconnect_reuse"))
    }
    fn cancel(&mut self, _op: OpId) -> io::Result<()> {
        Ok(())
    }

    fn poll(&mut self, out: &mut Vec<Completion>, timeout: Option<Duration>) -> io::Result<usize> {
        let mut pending = self.shared.pending_wakeups.lock().unwrap();
        if *pending == 0 {
            match timeout {
                Some(Duration::ZERO) => {}
                Some(t) => {
                    let (guard, _timed_out) =
                        self.shared.condvar.wait_timeout_while(pending, t, |p| *p == 0).unwrap();
                    pending = guard;
                }
                None => {}
            }
        }
        let n = *pending;
        if n > 0 {
            *pending = 0;
            // Collapse all pending posts into a single Wakeup completion —
            // the reactor drains the whole cross-thread queue regardless.
            out.push(Completion::Wakeup);
            return Ok(1);
        }
        Ok(0)
    }

    fn wakeup_handle(&self) -> Arc<dyn Wakeup> {
        Arc::new(PortableWakeup { shared: self.shared.clone() })
    }

    fn name(&self) -> &'static str {
        "portable-dev"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn poll_times_out_when_idle() {
        let mut b = PortableBackend::new();
        let mut out = Vec::with_capacity(4);
        let start = Instant::now();
        let n = b.poll(&mut out, Some(Duration::from_millis(30))).unwrap();
        assert_eq!(n, 0);
        assert!(start.elapsed() >= Duration::from_millis(25));
    }

    #[test]
    fn wakeup_interrupts_poll() {
        let mut b = PortableBackend::new();
        let wake = b.wakeup_handle();
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            wake.wake();
        });
        let mut out = Vec::with_capacity(4);
        let start = Instant::now();
        let n = b.poll(&mut out, Some(Duration::from_secs(5))).unwrap();
        assert_eq!(n, 1);
        assert_eq!(out[0], Completion::Wakeup);
        assert!(start.elapsed() < Duration::from_secs(1));
        t.join().unwrap();
    }

    #[test]
    fn wakeup_before_poll_is_not_lost() {
        let mut b = PortableBackend::new();
        b.wakeup_handle().wake();
        let mut out = Vec::with_capacity(4);
        let n = b.poll(&mut out, Some(Duration::from_secs(1))).unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn socket_ops_are_unsupported() {
        let mut b = PortableBackend::new();
        assert_eq!(b.post_accept(0).unwrap_err().kind(), io::ErrorKind::Unsupported);
        assert_eq!(b.post_recv(0, std::ptr::null_mut(), 0).unwrap_err().kind(), io::ErrorKind::Unsupported);
    }
}
