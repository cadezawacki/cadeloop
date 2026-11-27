//! The native access log (R-140).
//!
//! The logging-module access log costs a Python call per request on the
//! loop thread and a `LogRecord`, a format and a handler call per request
//! on a writer thread that needs the GIL to do any of it -- measured on
//! the plain-text benchmark, 37 us a request, five times the request
//! itself (139k requests/s without the log, 25k with it). This one never
//! runs Python on the request path:
//!
//! * A finished request is formatted, in the state cell, straight into a
//!   byte buffer: one line of text or one JSON object.
//! * Once a tick, the buffer is handed to a writer thread that owns the
//!   file. That thread never touches the interpreter, so a slow disk or a
//!   full pipe blocks it and nothing else -- neither the loop nor, through
//!   the GIL, the application.
//! * What the writer has not written yet is bounded ([`QUEUE_LIMIT`]).
//!   Past the bound, records are dropped and counted, and the count goes
//!   into the log itself when the writer catches up: a gap that says so
//!   beats a log that is quietly incomplete, and both beat unbounded
//!   memory.
//!
//! This module is the Python-free half: the formats, the buffer and the
//! writer. The binding layer adds the *tap* that hands the same records
//! to Python code in batches.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Bytes handed to the writer and not yet written, past which new records
/// are dropped (and counted).
pub const QUEUE_LIMIT: usize = 8 << 20;
/// How often the writer checks that the path still names the file it has
/// open (log rotation by rename).
const REOPEN_CHECK: Duration = Duration::from_secs(1);
/// How long `close` waits for the writer to finish what it was handed.
const CLOSE_WAIT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Format {
    /// `2026-11-27T15:38:01.123Z 127.0.0.1:41692 "GET /path" 200 0.12ms`
    Text,
    /// `{"ts":"...","client":"127.0.0.1:41692","method":"GET","target":"/path","status":200,"duration_ms":0.12}`
    Json,
}

impl Format {
    pub fn parse(name: &str) -> Option<Format> {
        match name {
            "text" => Some(Format::Text),
            "json" => Some(Format::Json),
            _ => None,
        }
    }
}

/// Formatted records on their way to a file.
pub struct LineLog {
    format: Format,
    /// Formatted records not yet handed to the writer.
    lines: Vec<u8>,
    /// How many records `lines` holds.
    pending: u64,
    writer: Option<Writer>,
    /// The date and time of `stamp_sec`, formatted once per second.
    stamp_sec: u64,
    stamp: [u8; 19],
    /// Records dropped since the log last said so.
    dropped: u64,
    /// Records dropped in all, for `stats()`.
    pub total_dropped: u64,
}

struct Writer {
    tx: Option<Sender<Vec<u8>>>,
    queued: Arc<AtomicUsize>,
    done: Receiver<()>,
}

impl LineLog {
    /// `path`: a file to append to, `-` for standard output, or None for
    /// a log nobody writes (its records are formatted nowhere).
    pub fn open(path: Option<&str>, format: Format) -> std::io::Result<LineLog> {
        let writer = match path {
            None => None,
            Some(path) => {
                let target = Target::open(path)?;
                let (tx, rx) = mpsc::channel::<Vec<u8>>();
                let (done_tx, done) = mpsc::channel::<()>();
                let queued = Arc::new(AtomicUsize::new(0));
                let counter = queued.clone();
                std::thread::Builder::new()
                    .name("cadeloop-access-log".into())
                    .spawn(move || write_loop(rx, target, counter, done_tx))?;
                Some(Writer { tx: Some(tx), queued, done })
            }
        };
        Ok(LineLog {
            format,
            lines: Vec::with_capacity(4096),
            pending: 0,
            writer,
            stamp_sec: u64::MAX,
            stamp: [0; 19],
            dropped: 0,
            total_dropped: 0,
        })
    }

    /// Format one finished request, stamped with the current time. Does
    /// nothing without a writer.
    #[inline]
    pub fn record(&mut self, client: &str, method: &str, target: &[u8], status: u16, dur_ms: f64) {
        if self.writer.is_some() {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
            self.format_line(now, client, method, target, status, dur_ms);
            self.pending += 1;
        }
    }

    fn format_line(
        &mut self,
        now: Duration,
        client: &str,
        method: &str,
        target: &[u8],
        status: u16,
        dur_ms: f64,
    ) {
        let secs = now.as_secs();
        if secs != self.stamp_sec {
            self.stamp_sec = secs;
            self.stamp = date_time(secs);
        }
        let millis = now.subsec_millis();
        let out = &mut self.lines;
        // Writing to a Vec cannot fail.
        match self.format {
            Format::Text => {
                out.extend_from_slice(&self.stamp);
                let _ = write!(out, ".{millis:03}Z {client} \"{method} ");
                escape_text(out, target);
                let _ = writeln!(out, "\" {status} {dur_ms:.2}ms");
            }
            Format::Json => {
                out.extend_from_slice(b"{\"ts\":\"");
                out.extend_from_slice(&self.stamp);
                let _ = write!(
                    out,
                    ".{millis:03}Z\",\"client\":\"{client}\",\"method\":\"{method}\",\"target\":\""
                );
                escape_json(out, target);
                let _ = writeln!(out, "\",\"status\":{status},\"duration_ms\":{dur_ms:.2}}}");
            }
        }
    }

    /// Anything to hand to the writer? One comparison, for the tick.
    #[inline]
    pub fn has_lines(&self) -> bool {
        !self.lines.is_empty()
    }

    /// Hand the records formatted so far to the writer. Never blocks.
    pub fn hand_off(&mut self) {
        if self.lines.is_empty() {
            return;
        }
        let Some(writer) = &self.writer else {
            self.lines.clear();
            self.pending = 0;
            return;
        };
        if writer.queued.load(Ordering::Relaxed) + self.lines.len() > QUEUE_LIMIT {
            self.dropped += self.pending;
            self.total_dropped += self.pending;
            self.lines.clear();
            self.pending = 0;
            return;
        }
        let mut chunk = Vec::with_capacity(self.lines.len() + 64);
        if self.dropped > 0 {
            match self.format {
                Format::Text => {
                    let _ = writeln!(chunk, "# access log fell behind: {} record(s) dropped", self.dropped);
                }
                Format::Json => {
                    let _ = writeln!(chunk, "{{\"dropped\":{}}}", self.dropped);
                }
            }
            self.dropped = 0;
        }
        chunk.extend_from_slice(&self.lines);
        self.lines.clear();
        self.pending = 0;
        writer.queued.fetch_add(chunk.len(), Ordering::Relaxed);
        if let Some(tx) = &writer.tx {
            let _ = tx.send(chunk);
        }
    }

    /// Stop the writer once it has written everything handed to it, or
    /// after [`CLOSE_WAIT`] if it is stuck. Blocking. Returns false if the
    /// writer did not finish.
    pub fn finish(&mut self) -> bool {
        self.hand_off();
        match self.writer.take() {
            Some(mut writer) => {
                drop(writer.tx.take()); // the writer drains its queue and exits
                writer.done.recv_timeout(CLOSE_WAIT).is_ok()
            }
            None => true,
        }
    }
}

/// `ip:port`, `[ipv6]:port`, or `-` for anything without one.
pub fn client_text(addr: Option<std::net::SocketAddr>) -> Box<str> {
    match addr {
        Some(a) => a.to_string().into_boxed_str(),
        None => "-".into(),
    }
}

/// A request target for the text format: printable ASCII as it is,
/// everything else -- and the two characters that would end or bend the
/// quoted field -- as `\xNN`. A target cannot forge a second line or a
/// status this way.
fn escape_text(out: &mut Vec<u8>, target: &[u8]) {
    for &b in target {
        if (0x20..0x7f).contains(&b) && b != b'"' && b != b'\\' {
            out.push(b);
        } else {
            let _ = write!(out, "\\x{b:02x}");
        }
    }
}

/// A request target as the inside of a JSON string, read as Latin-1 (the
/// same reading the logging path gives it).
fn escape_json(out: &mut Vec<u8>, target: &[u8]) {
    for &b in target {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            0x20..=0x7e => out.push(b),
            _ => {
                let _ = write!(out, "\\u{b:04x}");
            }
        }
    }
}

/// `YYYY-MM-DDTHH:MM:SS` (UTC) for a Unix time.
fn date_time(secs: u64) -> [u8; 19] {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // Days to a civil date (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let mut out = [0u8; 19];
    let mut cursor = &mut out[..];
    let _ = write!(
        cursor,
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        year.clamp(0, 9999),
        month,
        day,
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    );
    out
}

// ---------------------------------------------------------------------------
// the writer thread
// ---------------------------------------------------------------------------

enum Target {
    Stdout,
    File { path: PathBuf, file: File, checked: Instant },
}

impl Target {
    fn open(path: &str) -> std::io::Result<Target> {
        if path == "-" {
            return Ok(Target::Stdout);
        }
        let path = PathBuf::from(path);
        let file = append_to(&path)?;
        Ok(Target::File { path, file, checked: Instant::now() })
    }

    fn write(&mut self, chunk: &[u8]) {
        match self {
            Target::Stdout => {
                let mut out = std::io::stdout().lock();
                let _ = out.write_all(chunk);
                let _ = out.flush();
            }
            Target::File { path, file, checked } => {
                if checked.elapsed() >= REOPEN_CHECK {
                    *checked = Instant::now();
                    if moved_away(path, file) {
                        // Rotated out from under us: whatever tool renamed
                        // or removed the file expects the next line in a
                        // new one of the same name.
                        if let Ok(new) = append_to(path) {
                            *file = new;
                        }
                    }
                }
                // A failed write (disk full, file gone) loses this chunk
                // and nothing else: there is nowhere to report it but the
                // log that just failed.
                let _ = file.write_all(chunk);
            }
        }
    }
}

/// Open for appending. Each chunk is whole records written in one call,
/// and an append is atomic, so several workers can share a file without
/// their lines running into each other.
fn append_to(path: &PathBuf) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

/// Does `path` no longer name the file we have open?
#[cfg(unix)]
fn moved_away(path: &PathBuf, file: &File) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(path), file.metadata()) {
        (Ok(on_disk), Ok(open)) => on_disk.dev() != open.dev() || on_disk.ino() != open.ino(),
        (Err(_), _) => true,
        _ => false,
    }
}

/// Does `path` no longer name the file we have open? Without a stable way
/// to compare file identities here, "the name is gone" is the test: a
/// rotation that renames the file away is seen, one that swaps another
/// file in under the name within the same second is not.
#[cfg(not(unix))]
fn moved_away(path: &PathBuf, _file: &File) -> bool {
    std::fs::metadata(path).is_err()
}

fn write_loop(rx: Receiver<Vec<u8>>, mut target: Target, queued: Arc<AtomicUsize>, done: Sender<()>) {
    while let Ok(chunk) = rx.recv() {
        target.write(&chunk);
        queued.fetch_sub(chunk.len(), Ordering::Relaxed);
    }
    let _ = done.send(());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(format: Format, target: &[u8]) -> String {
        let mut log = LineLog::open(None, format).unwrap();
        log.format_line(
            Duration::new(1_795_793_881, 123_000_000),
            "127.0.0.1:41692",
            "GET",
            target,
            200,
            0.125,
        );
        String::from_utf8(log.lines).unwrap()
    }

    #[test]
    fn dates() {
        assert_eq!(&date_time(0), b"1970-01-01T00:00:00");
        assert_eq!(&date_time(951_782_400), b"2000-02-29T00:00:00");
        assert_eq!(&date_time(1_795_793_881), b"2026-11-27T15:38:01");
        assert_eq!(&date_time(4_102_444_799), b"2099-12-31T23:59:59");
    }

    #[test]
    fn text_line() {
        assert_eq!(
            text(Format::Text, b"/a?b=1"),
            "2026-11-27T15:38:01.123Z 127.0.0.1:41692 \"GET /a?b=1\" 200 0.12ms\n"
        );
    }

    #[test]
    fn text_line_cannot_be_forged() {
        let line = text(Format::Text, b"/x\" 200 0.00ms\r\n1.2.3.4 \"GET /\xff\\");
        assert_eq!(line.matches('\n').count(), 1);
        assert!(line.contains(r#"/x\x22 200 0.00ms\x0d\x0a1.2.3.4 \x22GET /\xff\x5c" 200"#), "{line}");
    }

    #[test]
    fn json_line() {
        assert_eq!(
            text(Format::Json, b"/a\"\\\x01\xe9"),
            "{\"ts\":\"2026-11-27T15:38:01.123Z\",\"client\":\"127.0.0.1:41692\",\"method\":\"GET\",\
             \"target\":\"/a\\\"\\\\\\u0001\\u00e9\",\"status\":200,\"duration_ms\":0.12}\n"
        );
    }

    #[test]
    fn writer_appends_and_follows_rotation() {
        let dir = std::env::temp_dir().join(format!("cadeloop-accesslog-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("access.log");
        let _ = std::fs::remove_file(&path);
        let mut log = LineLog::open(path.to_str(), Format::Text).unwrap();
        log.format_line(Duration::new(0, 0), "-", "GET", b"/one", 200, 1.0);
        log.pending = 1;
        log.hand_off();
        // Rotate: the writer must notice within its check interval.
        let rotated = dir.join("access.log.1");
        let deadline = Instant::now() + Duration::from_secs(5);
        while std::fs::read_to_string(&path).map(|s| s.is_empty()).unwrap_or(true) {
            assert!(Instant::now() < deadline, "first record never written");
            std::thread::sleep(Duration::from_millis(5));
        }
        std::fs::rename(&path, &rotated).unwrap();
        std::thread::sleep(REOPEN_CHECK + Duration::from_millis(100));
        log.format_line(Duration::new(0, 0), "-", "GET", b"/two", 200, 1.0);
        log.pending = 1;
        assert!(log.finish(), "the writer did not finish");
        assert!(std::fs::read_to_string(&rotated).unwrap().contains("/one"));
        let fresh = std::fs::read_to_string(&path).unwrap();
        assert!(fresh.contains("/two") && !fresh.contains("/one"), "{fresh}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_writer_that_falls_behind_drops_and_says_so() {
        let mut log = LineLog::open(None, Format::Text).unwrap();
        // A writer whose queue is already full, with nobody reading it.
        let (tx, _rx) = mpsc::channel::<Vec<u8>>();
        let (_done_tx, done) = mpsc::channel::<()>();
        let queued = Arc::new(AtomicUsize::new(QUEUE_LIMIT));
        log.writer = Some(Writer { tx: Some(tx), queued: queued.clone(), done });
        for _ in 0..3 {
            log.format_line(Duration::new(0, 0), "-", "GET", b"/", 200, 1.0);
            log.pending += 1;
        }
        log.hand_off();
        assert_eq!((log.dropped, log.total_dropped, log.lines.len()), (3, 3, 0));
        // It catches up: the next hand-off leads with the count.
        queued.store(0, Ordering::Relaxed);
        log.format_line(Duration::new(0, 0), "-", "GET", b"/later", 200, 1.0);
        log.pending += 1;
        log.hand_off();
        let chunk = String::from_utf8(_rx.try_recv().unwrap()).unwrap();
        assert!(chunk.starts_with("# access log fell behind: 3 record(s) dropped\n"), "{chunk}");
        assert!(chunk.contains("/later"));
        assert_eq!(log.dropped, 0);
    }
}
