//! HTTP/1.1 request parsing over vendored llhttp (R-080).
//!
//! Pure Rust + C — zero Python objects until dispatch (the pyshim layer
//! builds the ASGI scope from [`Request`] parts). Strict mode (all lenient
//! flags off — llhttp's default). Limits per R-080 are enforced inside the
//! callbacks; violations surface as [`ParseError`] with the right 4xx
//! status.
//!
//! Chunked transfer decoding is llhttp's job: `on_body` receives decoded
//! bytes. Keep-alive/pipelining (R-085): one `execute` call parses every
//! complete message in the buffer; completed requests queue in order.

use std::collections::VecDeque;
use std::ffi::c_void;
use std::os::raw::{c_char, c_int};

// ---- llhttp FFI (layout mirrors vendor/llhttp/llhttp.h exactly) --------

#[repr(C)]
pub struct Llhttp {
    _index: i32,
    _span_pos0: *mut c_void,
    _span_cb0: *mut c_void,
    error: i32,
    reason: *const c_char,
    error_pos: *const c_char,
    data: *mut c_void,
    _current: *mut c_void,
    content_length: u64,
    type_: u8,
    method: u8,
    http_major: u8,
    http_minor: u8,
    header_state: u8,
    lenient_flags: u16,
    upgrade: u8,
    finish: u8,
    flags: u16,
    status_code: u16,
    initial_message_completed: u8,
    settings: *mut c_void,
}

type Cb = Option<unsafe extern "C" fn(*mut Llhttp) -> c_int>;
type DataCb = Option<unsafe extern "C" fn(*mut Llhttp, *const c_char, usize) -> c_int>;

#[repr(C)]
#[derive(Default)]
pub struct Settings {
    on_message_begin: Cb,
    on_url: DataCb,
    on_status: DataCb,
    on_method: DataCb,
    on_version: DataCb,
    on_header_field: DataCb,
    on_header_value: DataCb,
    on_chunk_extension_name: DataCb,
    on_chunk_extension_value: DataCb,
    on_headers_complete: Cb,
    on_body: DataCb,
    on_message_complete: Cb,
    on_url_complete: Cb,
    on_status_complete: Cb,
    on_method_complete: Cb,
    on_version_complete: Cb,
    on_header_field_complete: Cb,
    on_header_value_complete: Cb,
    on_chunk_extension_name_complete: Cb,
    on_chunk_extension_value_complete: Cb,
    on_chunk_header: Cb,
    on_chunk_complete: Cb,
    on_reset: Cb,
}

extern "C" {
    fn llhttp_init(parser: *mut Llhttp, kind: c_int, settings: *const Settings);
    fn llhttp_execute(parser: *mut Llhttp, data: *const c_char, len: usize) -> c_int;
    fn llhttp_should_keep_alive(parser: *const Llhttp) -> c_int;
    fn llhttp_method_name(method: c_int) -> *const c_char;
    fn llhttp_errno_name(err: c_int) -> *const c_char;
}

const HTTP_REQUEST: c_int = 1;
const HPE_OK: c_int = 0;
const HPE_PAUSED_UPGRADE: c_int = 22;
/// Our limit violations (callbacks return -1 == HPE_USER... actually
/// llhttp maps nonzero callback returns to HPE_USER/HPE_CB_*).
const _HPE_USER: c_int = 24;

// ---- limits (R-080 defaults; configurable via serve cfg) ---------------

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_header_bytes: usize,
    pub max_headers: usize,
    pub max_url: usize,
    pub max_body: Option<usize>,
}

impl Default for Limits {
    fn default() -> Self {
        Limits { max_header_bytes: 64 * 1024, max_headers: 100, max_url: 8 * 1024, max_body: None }
    }
}

/// One fully parsed request, ready for ASGI scope construction.
#[derive(Debug)]
pub struct Request {
    pub method: &'static str,
    pub http_minor: u8,
    pub keep_alive: bool,
    /// llhttp saw `Connection: upgrade` + `Upgrade:` on this request
    /// (R-087: the engine decides whether to take the WebSocket path).
    pub upgrade: bool,
    /// Raw request-target bytes (R-081 raw_path; percent-decoding is the
    /// scope builder's job).
    pub url: Vec<u8>,
    /// Names already lower-cased (R-081).
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

impl Request {
    /// Bytes this parsed request retains, for the pipeline queue budget.
    /// Approximate by design: the point is bounding memory, not exact
    /// accounting.
    pub fn queued_size(&self) -> usize {
        self.url.len()
            + self.body.len()
            + self.headers.iter().map(|(n, v)| n.len() + v.len() + 4).sum::<usize>()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseError {
    /// Suggested response status (400 or 431/413).
    pub status: u16,
    pub reason: &'static str,
}

struct Acc {
    limits: Limits,
    url: Vec<u8>,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    field: Vec<u8>,
    value: Vec<u8>,
    in_value: bool,
    header_bytes: usize,
    body: Vec<u8>,
    completed: VecDeque<Request>,
    error: Option<ParseError>,
    /// A request head (request line + headers) is mid-receipt — drives
    /// the R-080 request-line/header timeout, which anchors at head
    /// start so drip-fed bytes (slowloris) cannot extend it. Body
    /// receipt is NOT head time (a slow upload falls under keep-alive
    /// idle policy instead).
    in_head: bool,
}

impl Acc {
    fn fail(&mut self, status: u16, reason: &'static str) -> c_int {
        self.error = Some(ParseError { status, reason });
        -1
    }

    fn commit_header(&mut self) {
        if self.in_value {
            let mut field = std::mem::take(&mut self.field);
            field.make_ascii_lowercase();
            let value = std::mem::take(&mut self.value);
            self.headers.push((field, value));
            self.in_value = false;
        }
    }
}

unsafe fn acc<'a>(p: *mut Llhttp) -> &'a mut Acc {
    unsafe { &mut *((*p).data.cast::<Acc>()) }
}

unsafe extern "C" fn on_url(p: *mut Llhttp, at: *const c_char, len: usize) -> c_int {
    let a = unsafe { acc(p) };
    if a.url.len() + len > a.limits.max_url {
        return a.fail(414, "request target too long");
    }
    a.url.extend_from_slice(unsafe { std::slice::from_raw_parts(at.cast::<u8>(), len) });
    0
}

unsafe extern "C" fn on_header_field(p: *mut Llhttp, at: *const c_char, len: usize) -> c_int {
    let a = unsafe { acc(p) };
    a.commit_header();
    a.header_bytes += len;
    if a.header_bytes > a.limits.max_header_bytes {
        return a.fail(431, "headers too large");
    }
    if a.headers.len() >= a.limits.max_headers {
        return a.fail(431, "too many headers");
    }
    a.field.extend_from_slice(unsafe { std::slice::from_raw_parts(at.cast::<u8>(), len) });
    0
}

unsafe extern "C" fn on_header_value(p: *mut Llhttp, at: *const c_char, len: usize) -> c_int {
    let a = unsafe { acc(p) };
    a.in_value = true;
    a.header_bytes += len;
    if a.header_bytes > a.limits.max_header_bytes {
        return a.fail(431, "headers too large");
    }
    a.value.extend_from_slice(unsafe { std::slice::from_raw_parts(at.cast::<u8>(), len) });
    0
}

unsafe extern "C" fn on_message_begin(p: *mut Llhttp) -> c_int {
    unsafe { acc(p) }.in_head = true;
    0
}

unsafe extern "C" fn on_headers_complete(p: *mut Llhttp) -> c_int {
    let a = unsafe { acc(p) };
    a.commit_header();
    a.in_head = false;
    0
}

unsafe extern "C" fn on_body(p: *mut Llhttp, at: *const c_char, len: usize) -> c_int {
    let a = unsafe { acc(p) };
    if let Some(cap) = a.limits.max_body {
        if a.body.len() + len > cap {
            return a.fail(413, "body too large");
        }
    }
    a.body.extend_from_slice(unsafe { std::slice::from_raw_parts(at.cast::<u8>(), len) });
    0
}

unsafe extern "C" fn on_message_complete(p: *mut Llhttp) -> c_int {
    let keep_alive = unsafe { llhttp_should_keep_alive(p) } != 0;
    let upgrade = unsafe { (*p).upgrade } != 0;
    let (method, minor) = unsafe { ((*p).method, (*p).http_minor) };
    let a = unsafe { acc(p) };
    let method_name = method_str(method);
    a.completed.push_back(Request {
        method: method_name,
        upgrade,
        http_minor: minor,
        keep_alive,
        url: std::mem::take(&mut a.url),
        headers: std::mem::take(&mut a.headers),
        body: std::mem::take(&mut a.body),
    });
    a.header_bytes = 0;
    0
}

fn method_str(m: u8) -> &'static str {
    unsafe {
        let ptr = llhttp_method_name(m as c_int);
        std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(ptr).to_bytes())
    }
}

fn errno_str(e: c_int) -> &'static str {
    unsafe {
        let ptr = llhttp_errno_name(e);
        std::str::from_utf8_unchecked(std::ffi::CStr::from_ptr(ptr).to_bytes())
    }
}

const SETTINGS: Settings = Settings {
    on_message_begin: Some(on_message_begin),
    on_url: Some(on_url),
    on_status: None,
    on_method: None,
    on_version: None,
    on_header_field: Some(on_header_field),
    on_header_value: Some(on_header_value),
    on_chunk_extension_name: None,
    on_chunk_extension_value: None,
    on_headers_complete: Some(on_headers_complete),
    on_body: Some(on_body),
    on_message_complete: Some(on_message_complete),
    on_url_complete: None,
    on_status_complete: None,
    on_method_complete: None,
    on_version_complete: None,
    on_header_field_complete: None,
    on_header_value_complete: None,
    on_chunk_extension_name_complete: None,
    on_chunk_extension_value_complete: None,
    on_chunk_header: None,
    on_chunk_complete: None,
    on_reset: None,
};

/// Per-connection HTTP/1.1 request parser.
pub struct HttpParser {
    raw: Box<Llhttp>,
    acc: Box<Acc>,
}

// SAFETY: thread-affine by loop contract; raw pointers reference the
// heap-pinned boxes moved together with the parser.
unsafe impl Send for HttpParser {}

impl HttpParser {
    pub fn new(limits: Limits) -> Self {
        let mut raw: Box<Llhttp> = Box::new(unsafe { std::mem::zeroed() });
        let mut acc = Box::new(Acc {
            limits,
            url: Vec::new(),
            headers: Vec::new(),
            field: Vec::new(),
            value: Vec::new(),
            in_value: false,
            header_bytes: 0,
            body: Vec::new(),
            completed: VecDeque::new(),
            error: None,
            in_head: false,
        });
        unsafe {
            llhttp_init(&mut *raw, HTTP_REQUEST, &SETTINGS);
            raw.data = (&mut *acc as *mut Acc).cast();
        }
        HttpParser { raw, acc }
    }

    /// Feed bytes; completed requests become available via `next_request`.
    /// Feed bytes. `Ok(Some(offset))` means the parser paused after an
    /// upgrade head (HPE_PAUSED_UPGRADE): bytes from `offset` onward are
    /// NOT HTTP — they belong to the upgraded protocol's stream (R-087).
    pub fn feed(&mut self, data: &[u8]) -> Result<Option<usize>, ParseError> {
        let rc = unsafe { llhttp_execute(&mut *self.raw, data.as_ptr().cast(), data.len()) };
        if rc == HPE_PAUSED_UPGRADE {
            let consumed = unsafe { (self.raw.error_pos).offset_from(data.as_ptr().cast()) };
            let consumed = consumed.clamp(0, data.len() as isize) as usize;
            return Ok(Some(consumed));
        }
        if rc != HPE_OK {
            if let Some(err) = self.acc.error {
                return Err(err);
            }
            let _ = errno_str(rc); // (available for logging)
            return Err(ParseError { status: 400, reason: "malformed request" });
        }
        Ok(None)
    }

    pub fn next_request(&mut self) -> Option<Request> {
        self.acc.completed.pop_front()
    }

    pub fn has_request(&self) -> bool {
        !self.acc.completed.is_empty()
    }

    /// A request head is currently mid-receipt (R-080 timeout phase).
    pub fn in_head(&self) -> bool {
        self.acc.in_head
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(input: &[u8]) -> Request {
        let mut p = HttpParser::new(Limits::default());
        p.feed(input).unwrap();
        p.next_request().expect("complete request")
    }

    #[test]
    fn parses_simple_get() {
        let r = parse_one(b"GET /path?x=1 HTTP/1.1\r\nHost: h\r\nUser-Agent: t\r\n\r\n");
        assert_eq!(r.method, "GET");
        assert_eq!(r.http_minor, 1);
        assert!(r.keep_alive);
        assert_eq!(r.url, b"/path?x=1");
        assert_eq!(
            r.headers,
            vec![(b"host".to_vec(), b"h".to_vec()), (b"user-agent".to_vec(), b"t".to_vec())]
        );
        assert!(r.body.is_empty());
    }

    #[test]
    fn parses_post_body_and_pipelined_next() {
        let mut p = HttpParser::new(Limits::default());
        p.feed(b"POST /a HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n\r\nhelloGET /b HTTP/1.1\r\nHost: h\r\n\r\n")
            .unwrap();
        let a = p.next_request().unwrap();
        assert_eq!((a.method, a.body.as_slice()), ("POST", b"hello".as_slice()));
        let b = p.next_request().unwrap();
        assert_eq!((b.method, b.url.as_slice()), ("GET", b"/b".as_slice()));
        assert!(p.next_request().is_none());
    }

    #[test]
    fn parses_chunked_body_decoded() {
        let r = parse_one(
            b"POST /c HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n7\r\n world!\r\n0\r\n\r\n"
                [..].as_ref(),
        );
        assert_eq!(r.body, b"hello world!"[..].to_vec());
    }

    #[test]
    fn split_feeds_accumulate() {
        let mut p = HttpParser::new(Limits::default());
        for chunk in [b"GE".as_ref(), b"T / HT", b"TP/1.1\r\nHos", b"t: h\r\n", b"\r\n"] {
            p.feed(chunk).unwrap();
        }
        let r = p.next_request().unwrap();
        assert_eq!(r.method, "GET");
    }

    #[test]
    fn http10_defaults_to_close() {
        let r = parse_one(b"GET / HTTP/1.0\r\nHost: h\r\n\r\n");
        assert_eq!(r.http_minor, 0);
        assert!(!r.keep_alive);
    }

    #[test]
    fn malformed_is_a_400() {
        let mut p = HttpParser::new(Limits::default());
        let err = p.feed(b"NOT A REQUEST\r\n\r\n").unwrap_err();
        assert_eq!(err.status, 400);
    }

    #[test]
    fn url_limit_is_414() {
        let mut p = HttpParser::new(Limits { max_url: 16, ..Default::default() });
        let err = p.feed(b"GET /aaaaaaaaaaaaaaaaaaaaaaaaaaaaa HTTP/1.1\r\n\r\n").unwrap_err();
        assert_eq!(err.status, 414);
    }

    #[test]
    fn header_count_limit_is_431() {
        let mut p = HttpParser::new(Limits { max_headers: 2, ..Default::default() });
        let err = p.feed(b"GET / HTTP/1.1\r\nA: 1\r\nB: 2\r\nC: 3\r\n\r\n").unwrap_err();
        assert_eq!(err.status, 431);
    }

    #[test]
    fn body_limit_is_413() {
        let mut p = HttpParser::new(Limits { max_body: Some(4), ..Default::default() });
        let err = p.feed(b"POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\n0123456789").unwrap_err();
        assert_eq!(err.status, 413);
    }
}
